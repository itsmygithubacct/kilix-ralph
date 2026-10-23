"""Regression checks for restart, cancellation and service diagnostics."""
import argparse
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from kilix_ralph.__main__ import dispatch, service
from kilix_ralph.claude import ClaudeBridge
from kilix_ralph.store import Error, Store
from kilix_ralph.worker import Worker


class RetainedIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / 'state')
        self.addCleanup(self.store.close)

    def test_provider_custody_survives_contradiction_and_transport_failure(self):
        self.store.add_target('owned', [sys.executable, '-c', 'pass'])
        first = self.store.enqueue('owned', 'private text', 'sender')
        second = self.store.enqueue('owned', 'next', 'sender')
        self.store.claim(first['id'])
        receipt = {'evidence': {'queue_id': 'native'}}
        self.store.outcome(first['id'], 'provider_queued', 'owned', receipt)
        calls = []
        def transport(argv, request, timeout):
            calls.append(request['operation'])
            if len(calls) == 1:
                raise Error('unreachable')
            return {'operation_id': first['id'], 'target': 'owned',
                    'status': 'uncertain' if len(calls) == 2 else 'not_submitted', 'no_effects': True}
        worker = Worker(self.store, transport=transport)
        for _ in range(3):
            worker.once()
            row = self.store.message(first['id'])
            self.assertEqual(row['status'], 'provider_queued')
            self.assertEqual(row['receipt'], receipt)
            with self.assertRaises(Error):
                self.store.cancel(first['id'])
            self.assertFalse(self.store.claim(second['id']))
        self.assertEqual(calls, ['reconcile'] * 3)

    def test_invalid_run_poll_leaves_paused_loop_unchanged(self):
        self.store.add_target('mail')
        loop = self.store.create_loop('mail', 'next', 'sender', 60, max_wakes=1)
        args = argparse.Namespace(command='run', loop_id=loop['id'], poll=0, once=True)
        with self.assertRaises(Error):
            dispatch(self.store, args)
        self.assertEqual(self.store.loop(loop['id']), loop)

    def test_restart_rechecks_receipts_after_service_lock(self):
        # A bridge may be constructed while the previous owner is still active.
        bridge = ClaudeBridge(self.store, 'owned', self.tmp.name,
                              argv=[sys.executable, '-c', 'pass'])
        self.addCleanup(bridge.close)
        bridge.ledger.intent('owned', 'late', 'a' * 64, 'unresolved')
        with patch('kilix_ralph.claude.subprocess.Popen', side_effect=AssertionError('must not launch unresolved session')) as launch:
            bridge.serve(stopping=lambda: True)
        launch.assert_not_called()
        self.assertIsNotNone(bridge.blocked)
        self.assertFalse(bridge.endpoint.exists())

    def test_completed_receipt_refresh_selects_resume_under_lock(self):
        with patch('kilix_ralph.claude.shutil.which', return_value='/fixture/provider'):
            bridge = ClaudeBridge(self.store, 'owned', self.tmp.name)
            self.addCleanup(bridge.close)
            self.assertIn('--session-id', bridge.argv)
            bridge.ledger.intent('owned', 'late', 'a' * 64, 'completed')
            bridge.receipt('late', 'completed', result='fixture')
            with patch('kilix_ralph.claude.subprocess.Popen', side_effect=OSError('fixture stop')) as launch:
                with self.assertRaisesRegex(OSError, 'fixture stop'):
                    bridge.serve(stopping=lambda: True)
            argv = launch.call_args.args[0]
            self.assertIn('--resume', argv)
            self.assertNotIn('--session-id', argv)
            self.assertFalse(bridge.endpoint.exists())

    def test_continuous_service_reports_errors_without_prompt_or_repetition(self):
        args = argparse.Namespace(command='serve', poll=.05, once=False)
        calls = []
        def once():
            calls.append(1)
            if len(calls) == 3:
                raise Error('test stop')
            return {'generated': [], 'targets': [{'target': 'owned', 'id': 'message',
                'status': 'queued', 'text': 'PRIVATE PROMPT', 'error': 'PRIVATE DIAGNOSTIC'}]}
        output = io.StringIO()
        with patch('kilix_ralph.__main__.Worker') as worker, contextlib.redirect_stderr(output):
            worker.return_value.once.side_effect = once
            with self.assertRaisesRegex(Error, 'test stop'):
                service(self.store, args)
        lines = output.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0])['target'], 'owned')
        self.assertNotIn('PRIVATE', output.getvalue())
