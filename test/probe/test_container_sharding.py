

from unittest import main
from uuid import uuid4
import datetime, time

from swiftclient import client

from test.probe.common import ReplProbeTest 


class TestContainerShardingOff(ReplProbeTest):
    
    def test_check_sharding(self):
        self.test_check_sharding_with_objects(101)
        self.test_check_sharding_with_objects(500)

    def test_check_sharding_with_objects(self, obj_count):
    #Create container, fill with objects
        #import pdb; pdb.set_trace()
        con = 'container-%s' % uuid4()
        client.put_container(self.url, self.token, con)
        for i in xrange(0, obj_count):
            obj = 'obj%s' % i
            client.put_object(self.url, self.token, con, obj, '1')
        #shard container    
        self.shard_if_needed(self.account, con)
        
        for i in xrange(0, obj_count):
            obj = 'obj%s' % i
            client.delete_object(self.url, self.token, con, obj)

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
