"""Runner safety checks; no Docker, network or real configuration is used."""
import argparse
from contextlib import ExitStack, redirect_stderr, redirect_stdout
import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import urllib.parse

import mixed


ENV = {'DB_USER': 'fixture', 'DB_PASS': 'do-not-print:@/?', 'DB_NAME': 'fixture', 'DB_HOST_PORT': '35432'}
CONTAINER = {'Id': 'owned-container', 'Image': 'fixture-image',
             'Config': {'Labels': {'com.docker.compose.project': 'zchat-load', 'com.docker.compose.service': 'db'}},
             'HostConfig': {'Memory': 1024, 'NanoCpus': 2_000_000_000,
                            'PortBindings': {'5432/tcp': [{'HostIp': '127.0.0.1', 'HostPort': '35432'}]}},
             'State': {'Running': False}}


class RunnerSafety(unittest.TestCase):
    def test_ownership_and_loopback_required(self):
        self.assertEqual(mixed.validate_container(CONTAINER, ENV)['Memory'], 1024)
        for where in ['project', 'service', 'wildcard', 'port']:
            value = copy.deepcopy(CONTAINER)
            if where in ['project', 'service']:
                value['Config']['Labels']['com.docker.compose.' + where] = 'foreign'
            else:
                value['HostConfig']['PortBindings']['5432/tcp'][0]['HostIp' if where == 'wildcard' else 'HostPort'] = '0.0.0.0' if where == 'wildcard' else '5432'
            with self.assertRaises(RuntimeError):
                mixed.validate_container(value, ENV)

    def test_credentials_encoded_in_private_dsn(self):
        parsed = urllib.parse.urlparse(mixed.connection_url(ENV, 'synthetic'))
        self.assertEqual(parsed.hostname, '127.0.0.1')
        self.assertEqual(urllib.parse.unquote(parsed.password), ENV['DB_PASS'])
        self.assertEqual(parsed.port, 35432)

    def test_invalid_limits_fail_before_reading_lab_or_building(self):
        for args in [['--seconds', '0'], ['--seconds', '31'], ['--workers', '65'], ['--race', '--seconds', '4']]:
            with patch('sys.argv', ['mixed.py', *args]), patch.object(mixed.lab, 'config') as config, redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    mixed.main()
                config.assert_not_called()

    def test_concurrent_runner_cannot_take_or_release_other_runner_lock(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(mixed.lab, 'LAB', Path(directory)):
            with mixed.lab_lock():
                with self.assertRaises(RuntimeError):
                    with mixed.lab_lock():
                        self.fail('second runner acquired active lock')
            with mixed.lab_lock():
                pass

    def simulate(self, *, exit_code=0, running=False, wrong_marker=False, foreign=False):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            root = Path(directory)
            value = copy.deepcopy(CONTAINER)
            value['State']['Running'] = running
            if foreign:
                value['Config']['Labels']['com.docker.compose.project'] = 'foreign'
            events = []
            marker = None

            def command(args, **kwargs):
                events.append(args[:])
                if args[:2] == ['docker', 'inspect']:
                    return json.dumps([value])
                return 'fixture'

            def sql(env, text):
                nonlocal marker
                self.assertEqual(env, ENV)
                events.append(text)
                if text.startswith('COMMENT'):
                    marker = text.split(" IS '")[1].rstrip("'")
                if text.startswith('SELECT shobj_description'):
                    return 'foreign-marker' if wrong_marker else marker
                if 'json_build_object' in text:
                    return '{}'
                return '1'

            def process(args, log, timeout, env=None):
                events.append('compile' if args[0] == 'go' else 'test')
                if args[0] == 'go':
                    Path(args[args.index('-o') + 1]).write_bytes(b'fixture executable')
                    return 0
                self.assertIn('127.0.0.1:35432/zchat_mixed_', env['ZCHAT_TEST_DATABASE_URL'])
                self.assertEqual(timeout, 380)
                return exit_code

            stack.enter_context(patch.object(mixed, 'ROOT', root))
            stack.enter_context(patch.object(mixed.lab, 'LAB', root / 'out/load'))
            stack.enter_context(patch.object(mixed.lab, 'source_digest', return_value='digest'))
            stack.enter_context(patch.object(mixed, 'command', side_effect=command))
            stack.enter_context(patch.object(mixed, 'psql', side_effect=sql))
            stack.enter_context(patch.object(mixed, 'bounded_process', side_effect=process))
            stack.enter_context(patch.object(mixed, 'sample', side_effect=lambda stop, *_: stop.wait(1)))
            stack.enter_context(redirect_stdout(io.StringIO()))
            error = None
            result = None
            try:
                result = mixed.execute(ENV, argparse.Namespace(race=False, admission='enabled'), 1, 1)
            except RuntimeError as err:
                error = err
            paths = list((root / 'out/load/mixed').glob('*/metadata.json'))
            self.assertEqual(len(paths), 1)
            metadata = json.loads(paths[0].read_text())
            self.assertNotIn(ENV['DB_PASS'], paths[0].read_text())
            self.assertFalse((paths[0].parent / 'mixed.test').exists())
            return result, error, events, metadata

    def test_failed_test_removes_only_marked_database_and_restores_stopped_container(self):
        result, error, events, metadata = self.simulate(exit_code=3)
        self.assertEqual(result, 3)
        self.assertIsNone(error)
        drops = [e for e in events if isinstance(e, str) and e.startswith('DROP')]
        self.assertEqual(len(drops), 1)
        self.assertIn(metadata['ownedDatabase'], drops[0])
        self.assertLess(events.index('test'), events.index(drops[0]))
        self.assertTrue(metadata['disposableDatabaseRemoved'])
        self.assertTrue(metadata['databaseRestoredStopped'])
        self.assertTrue(metadata['cleanupComplete'])

    def test_existing_running_database_is_not_stopped(self):
        result, error, events, metadata = self.simulate(running=True)
        self.assertEqual(result, 0)
        self.assertIsNone(error)
        self.assertFalse(any(isinstance(e, list) and e[:2] in [['docker', 'start'], ['docker', 'stop']] for e in events))
        self.assertTrue(metadata['cleanupComplete'])

    def test_changed_database_marker_refuses_delete_and_records_incomplete_cleanup(self):
        _, error, events, metadata = self.simulate(wrong_marker=True)
        self.assertIsInstance(error, RuntimeError)
        self.assertFalse(any(isinstance(e, str) and e.startswith('DROP') for e in events))
        self.assertFalse(metadata['cleanupComplete'])
        self.assertEqual(metadata['cleanupError'], 'RuntimeError')

    def test_foreign_container_is_not_started_or_changed(self):
        _, error, events, metadata = self.simulate(foreign=True)
        self.assertIsInstance(error, RuntimeError)
        self.assertFalse(any(isinstance(e, str) and e.startswith(('CREATE', 'DROP', 'COMMENT')) for e in events))
        self.assertFalse(any(isinstance(e, list) and e[:2] == ['docker', 'start'] for e in events))


if __name__ == '__main__':
    unittest.main()
