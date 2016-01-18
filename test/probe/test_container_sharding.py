

from unittest import main
from uuid import uuid4
import datetime, time
from swift.common.internal_client import InternalClient
from swiftclient import client
from swift.container.sharder import ic_conf_body
from test.probe.common import * 
from swift.common.wsgi import ConfigString

class TestContainerShardingOff(ReplProbeTest):
    def setUp(self):
        super(TestContainerShardingOff, self).setUp()
        self.swift = InternalClient(ConfigString(ic_conf_body), 'Probe Test', 3)

    def test_tree_structure(self):
        container = 'container-%s' % uuid4()
        client.put_container(self.url, self.token, container)

        for name in self._objects(0, 500): 
            client.put_object(self.url, self.token, container, name, name)     
        self.shard_if_needed(self.account, container)

        path = self.swift.make_path(self.account, container) + \
            '?nodes=pivot&format=json'
        resp = self.swift.make_request('GET', path, {}, acceptable_statuses=(2,))   

        tree = PivotTree()
        pivots = {}
        for pivot in json.loads(resp.body):                     
            tree.add(pivot['name'])
            pivots['name'] = pivot

        def consistent_levels(node, level=0):
            if node is None:
                return
            self.assertEqual(node.level, level)
            self.assertEqual(node.level, pivots[node.key])
            consistent_levels(node.left, level + 1) 
            consistent_levels(node.right, level + 1)

        def consistent_ordering(node): 
            if node is None:
                return
            if node.left:   
                self.assertTrue(node.key >= node.left.key)
                consistent_ordering(node.left)
            if node.right:
                self.assertTrue(node.key < node.right.key)
                consistent_ordering(node.right)

        consistent_levels(tree._root)
        consistent_ordering(tree._root)
    def test_check_deleted(self):
        con = 'container-%s' % uuid4()
        client.put_container(self.url, self.token, con)
        for name in self._objects(0, 101):
            client.put_object(self.url, self.token, con, name, '1')
        #import pdb; pdb.set_trace()
        self.shard_if_needed(self.account, con)
        #get pivot points 
        path = self.swift.make_path(self.account, con) + \
            '?nodes=pivot&format=json'
        resp = self.swift.make_request('GET', path, {}, acceptable_statuses=(2,))
        pivot_containers = [x['name'] for x in json.loads(resp.body)]
        #delete objects and container
        for name in self._objects(0, 101):
            client.delete_object(self.url, self.token, con, name)

        client.delete_container(self.url, self.token, con)
        for name in pivot_containers:
            for weight in (-1, 1):
                shardaccount, shardcontainer = pivot_to_pivot_container(self.account, con , name, weight)
                path = self.swift.make_path(shardaccount, shardcontainer)
                resp = self.swift.make_request('GET', path, {}, acceptable_statuses=(3,4,5))
                    
    def test_check_sharding_101(self):
        self._check_sharding_with_objects(101, datetime.timedelta(minutes=2))

    def test_check_sharding_500(self):
        self._check_sharding_with_objects(500, datetime.timedelta(minutes=3))

    def _check_sharding_with_objects(self, obj_count, timeout):
        #Create container, fill with objects
        con = 'container-%s' % uuid4()
        client.put_container(self.url, self.token, con)
        for name in self._objects(0, obj_count):
            client.put_object(self.url, self.token, con, name, '1')
        self.shard_if_needed(self.account, con, timeout=timeout)

        for name in self._objects(0, obj_count):
            client.delete_object(self.url, self.token, con, name)

        client.delete_container(self.url, self.token, con)

    def _wait_for(self, check, error_message,
                  timeout=datetime.timedelta(minutes=3),
                  interval=datetime.timedelta(seconds=10)):
        self.get_to_final_state()

        try_until = time.time() + timeout.total_seconds()
        while True:
            time.sleep(interval.total_seconds())
            if check():
                break
            if time.time() > try_until:
                print error_message
                raise Exception(error_message)

    def _objects(self, minimum, maximum):
        for i in xrange(minimum, maximum):
            yield 'object-%s' % format(i, '03')

    def test_Basic_CRUD(self):
        # + Create container
        # + Upload 200 Objects
        # + shard_if_needed at 100 objects
        # + Delete 50 objects
        # + check objects deleted
        # + rewrite 20 objects
        # + check can still access
        # + Upload 300 objects
        # + shard_if_needed
        # + check all objects still exist and no new objects
        # - delete the container
        # - check it is deleted
        shard_container_size = 100

        # Create a container
        container = 'container-%s' % uuid4()
        cpart, cnodes = self.container_ring.get_nodes(self.account, container)
        client.put_container(self.url, self.token, container)

        # Create all objects and force a shard to occur
        for name in self._objects(0, 200):
            client.put_object(self.url, self.token, container, name, name)
        self.shard_if_needed(self.account, container,
                             shard_container_size=shard_container_size)
        #import pdb; pdb.set_trace()

        # Delete objects
        for name in self._objects(0, 50):
            try:
                client.delete_object(self.url, self.token, container, name)
            except Exception as e:
                print e, '\n\n\n\n\n\n\n'

        def object_count():
            resp = client.head_container(self.url, self.token, container)
            count = resp.get('X-Container-Object-Count')
            return count == 150

        self._wait_for(object_count, 'Predicted number of objects after a delete did not occur after 5 minutes')

        # Get all the etags and then update the contents of the objects
        etags = []
        for name in self._objects(50, 100):
            resp = client.head_object(self.url, self.token, container, name)
            etags.append(resp.get('ETag'))
        for name in self._objects(50, 100):
            data = str(i + 1000)
            client.put_object(self.url, self.token, container, name, data)

        def etags_differ():
            new_tags = []
            for name in self._objects(50, 100):
                resp = client.head_object(self.url, self.token, container,
                                          name)
                new_tags.append(resp.get('ETag'))
            return len(set(new_tag).intersection(set(etags))) == 0

        self._wait_for(etags_differ, 'contents update did not occur within '
                       '5 minutes')

        # add 300 objects and shard
        for name in self._objects(200, 500):
            client.put_object(self.url, self.token, container, name, name)
        self.shard_if_needed(self.account, container,
                             shard_container_size=shard_container_size)

        print 'DONE!'
        client.delete_container(self.url, self.token, container)

        with self.assertRaises(Exception):
            client.get_container(self.url, self.token, container)



class TestContainerShardingOn(TestContainerShardingOff):
    sharding_enabled = True

