"""Exercise the normal Rover caller without sending physical commands."""
import json
import os
from pathlib import Path
import unittest
from urllib.error import HTTPError
from tempfile import TemporaryDirectory
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'mac'))
from rover_routes import ControllerRoutes, ResourceMoved

LOCAL, PEER = '1' * 64, '2' * 64
CONTRACT, RESOURCE = 'device.outputs.stream:1', 'relay6ch.example'


class RoutingTest(unittest.TestCase):
    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.home = Path(tmp.name)
        cfg = self.home / 'controllers.json'
        cfg.write_text(json.dumps({'authorities': [PEER, LOCAL]}))
        env = patch.dict(os.environ, {'ROVER_CONTROLLERS_FILE': str(cfg)})
        env.start()
        self.addCleanup(env.stop)
        self.offers = {LOCAL: [], PEER: []}
        self.calls = []
        self.remote = self.client()

    def client(self):
        return ControllerRoutes(LOCAL, self.home, self.request)

    def offer(self, host, resource=RESOURCE, admitted=True, state='ready'):
        self.offers[host].append(dict(resource=resource, admitted=admitted, state=state))

    def request(self, host, op, args):
        self.calls.append((host, op))
        if op == 'cap.discover':
            if isinstance(self.offers[host], Exception):
                raise self.offers[host]
            if self.offers[host] is None:
                raise TimeoutError('controller offline')
            return {'contracts': [{'id': CONTRACT, 'implementations': self.offers[host]}]}
        if op == 'cap.acquire':
            self.acquire_args = dict(args)
            return {'id': 'lease', 'state': 'bound'}
        if op == 'stream.open':
            return {'id': 'session'}
        if op == 'stream.attach':
            raise TimeoutError('previous owner offline')
        if op == 'cap.submit':
            raise TimeoutError('response lost')
        if op == 'cap.status':
            return {'state': 'uncertain'}
        return {'state': 'released'}

    def acquire(self):
        return self.remote.call('cap.acquire', {'contract': CONTRACT, 'resource': RESOURCE})

    def test_peer_then_local_for_new_acquisitions(self):
        self.offer(PEER)
        self.offer(LOCAL, resource='relay6ch.some-other-board')
        self.offer(LOCAL, admitted=False)
        self.acquire()
        self.assertEqual(self.calls[-1], (PEER, 'cap.acquire'))
        self.remote.call('stream.open', {'lease': 'lease'})
        self.offers[PEER] = None
        self.offer(LOCAL)
        with self.assertRaises(ResourceMoved):
            self.remote.call('stream.attach', {'session': 'session', 'role': 'server'})
        self.assertNotIn((LOCAL, 'stream.attach'), self.calls)
        self.remote.call('cap.release', {'lease': 'lease'})
        self.assertEqual(self.calls[-1], (PEER, 'cap.release'))
        self.acquire()
        self.assertEqual(self.calls[-1], (LOCAL, 'cap.acquire'))

    def test_no_dispatch_to_unavailable_or_unrelated_resources(self):
        self.offer(LOCAL, state='offline')
        self.offers[PEER] = None
        with self.assertRaisesRegex(ConnectionError, 'No admitted offer'):
            self.acquire()
        self.assertTrue(all(op == 'cap.discover' for _, op in self.calls))

    def test_lost_submission_stays_with_original_owner_across_restart(self):
        self.offer(PEER)
        args = dict(operation='immutable-command', contract=CONTRACT, resource=RESOURCE)
        with self.assertRaises(TimeoutError):
            self.remote.call('cap.submit', args)
        self.offers[PEER] = None
        self.offer(LOCAL)
        recovered = self.client()
        with self.assertRaises(TimeoutError):
            recovered.call('cap.submit', args)
        self.assertEqual(self.calls[-1], (PEER, 'cap.submit'))
        self.assertEqual(recovered.call('cap.status', {'operation': args['operation']})['state'], 'uncertain')
        self.assertEqual(self.calls[-1], (PEER, 'cap.status'))

    def test_older_peer_can_grant_its_existing_exact_resource_without_queueing(self):
        self.offers[PEER] = HTTPError('https://gateway/v1/call', 403, 'Permission denied', {}, None)
        self.acquire()
        self.assertEqual(self.calls[-1], (PEER, 'cap.acquire'))
        self.assertEqual(self.acquire_args, dict(contract=CONTRACT, resource=RESOURCE, enqueue=False))
        with self.assertRaises(ConnectionError):
            self.remote.call('cap.submit', dict(contract=CONTRACT, resource=RESOURCE, operation='x'))
        self.assertNotIn((PEER, 'cap.submit'), self.calls)
