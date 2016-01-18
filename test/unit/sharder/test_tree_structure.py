#!/usr/bin/python


from probe.common import *
from swiftclient import client
from uuid import uuid4
from swift.common import internal_client
#from swift.common.wsgi import ConfigString
from swift.common import *
from swift.common.wsgi import *

class Test_Pivot_Tree(self):
    
    def test_tree_structure(self):
        acct_cont_required_replicas = 3
        acct_cont_required_devices = 4
        obj_required_replicas = 3
        obj_required_devices = 4
        policy_requirements = {'policy_type': REPL_POLICY}
        pids = {}
        ipport2server = {}
        proxy_ipport = ('127.0.0.1', 8080)
        configs = defaultdict(dict)

        account_ring = get_ring(
            'account',
            acct_cont_required_replicas,
            acct_cont_required_devices,
            ipport2server=ipport2server,
            config_paths=configs)
        container_ring = get_ring(
            'container',
            acct_cont_required_replicas,
            acct_cont_required_devices,
            ipport2server=ipport2server,
            config_paths=configs)
        policy = get_policy(**policy_requirements)
        object_ring = get_ring(
            policy.ring_name,
            obj_required_replicas,
            obj_required_devices,
            server='object',
            ipport2server=ipport2server,
            config_paths=configs)

        for ipport in ipport2server:
            check_server(ipport, ipport2server, pids)
        ipport2server[proxy_ipport] = 'proxy'
        url, token, account = check_server(proxy_ipport, ipport2server, pids)
        container_sharder = Manager(['container-sharder'])
        container_sharder.shard_container_size = 10
        
        def _objects(minimum, maximum):
            for i in xrange(minimum, maximum):
                yield 'object-%s' % format(i, '03')

        container = 'container-%s' % uuid4()
        print container
        cpart, cnodes = container_ring.get_nodes(account, container)
        client.put_container(url, token, container)

        for name in _objects(0, 100):
            print "  %s" % name
            client.put_object(url, token, container, name, name)
        print container

        config_string = '\n'.join(line.strip() for line in """
        [DEFAULT]
        swift_dir = /etc/swift

        [pipeline:main]
        pipeline = catch_errors cache proxy-server

        [app:proxy-server]
        use = egg:swift#proxy

        [filter:cache]
        use = egg:swift#memcache

        [filter:catch_errors]
        use = egg:swift#catch_errors""".split('\n'))
        swift = internal_client.InternalClient(ConfigString(config_string), 'test', 1)
        path = swift.make_path(account, container)
        resp = swift.make_request('POST', path, {'X-Container-Sharding': 'On'},
                                  acceptable_statuses=(2,))

        import time
        for i in range(0, 2):
            container_sharder.start(wait=True, once=True)
            time.sleep(10)

        pivot_tree = self._get_pivot_tree(swift, account, container)

        # may not be sharded yet
        #if pivot_tree is None:
        #    continue

        #import pdb; pdb.set_trace()

        # get all the sharded container accounts and container names
        shard_containers = []
        for leaf, weight in pivot_tree.leaves_iter():
            shard = pivot_to_pivot_container(account, container,
                     leaf.key, weight)
            shard_containers.append(shard)

            path = swift.make_path(account, container)
            #resp = swift.make_request('POST', path, {'X-Container-Sharding': 'On'},
            #                          acceptable_statuses=(2,))
            resp = swift.make_request('GET', path, {}, acceptable_statuses=(2,))
            resp_account = int(resp.headers['X-Backend-Shard-Account'])
            resp_container = int(resp.headers['X-Backend-Shard-Container'])
            resp_pivot = int(resp.headers['X-Container-Sysmeta-Shard-Pivot'])
            resp_obj_count = int(resp.headers['X-Container-Object-Count'])
            print resp_account, resp_container, resp_pivot, resp_obj_count

