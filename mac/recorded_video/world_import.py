"""Durable producer handoff; only the running atlas consumes geometry.

Store immutable native checkpoints first. This queue contains references only.
Producers may open this queue, but must never open another WorldAtlas writer.
"""
from contextlib import contextmanager
import json
import sqlite3
from pathlib import Path


class WorldImportQueue:
    def __init__(self, state_dir):
        self.path = Path(state_dir) / 'world-imports.sqlite3'
        with self._connect() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS imports (
                id TEXT PRIMARY KEY, map_id TEXT NOT NULL, checkpoint_id TEXT NOT NULL,
                state TEXT NOT NULL, receipt TEXT, error TEXT)''')

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(str(self.path), timeout=30)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA journal_mode=WAL')
        try:
            with db:
                yield db
        finally:
            db.close()

    def get(self, import_id):
        with self._connect() as db:
            row = db.execute('SELECT * FROM imports WHERE id=?', (import_id,)).fetchone()
        if row is None:
            return None
        value = dict(row)
        if value['receipt']:
            value['receipt'] = json.loads(value['receipt'])
        return value

    def submit(self, import_id, map_id, checkpoint_id):
        for value in (import_id, map_id, checkpoint_id):
            if not isinstance(value, str) or not value or len(value) > 256:
                raise ValueError('Import, map and checkpoint IDs must be nonempty strings of at most 256 characters')
        with self._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT map_id,checkpoint_id FROM imports WHERE id=?', (import_id,)).fetchone()
            if row is not None and tuple(row) != (map_id, checkpoint_id):
                raise ValueError('Import ID already names another checkpoint')
            db.execute("INSERT OR IGNORE INTO imports VALUES(?,?,?,'queued',NULL,NULL)",
                       (import_id, map_id, checkpoint_id))
        return self.get(import_id)

    def drain_one(self, store, atlas, active_map_id):
        # One owner thread calls this. A crash in integrating retries the SAME
        # map/checkpoint after restart; no extra map or checkpoint is created.
        with self._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute("SELECT * FROM imports WHERE state IN ('queued','integrating') ORDER BY rowid LIMIT 1").fetchone()
            if row is None:
                return None
            db.execute("UPDATE imports SET state='integrating' WHERE id=?", (row['id'],))
        try:
            if row['map_id'] == active_map_id:
                raise ValueError('Imported submap must not replace the active rover map')
            checkpoint = store.latest_backend_checkpoint(row['map_id'])
            if (checkpoint.get('availability') != 'available' or
                    checkpoint.get('map_id') != row['map_id'] or
                    checkpoint.get('checkpoint_id') != row['checkpoint_id']):
                raise ValueError('Finalized checkpoint unavailable or superseded')
            atlas.update(checkpoint)
            status = atlas.status()
            submap = next(s for s in status['submaps'] if s['map_id'] == row['map_id'])
            receipt = {'world_id': status['world_id'], 'world_revision': status['revision'],
                       'map_id': row['map_id'], 'checkpoint_id': row['checkpoint_id'],
                       'alignment': submap['alignment']}
            with self._connect() as db:
                db.execute("UPDATE imports SET state='completed',receipt=?,error=NULL WHERE id=?",
                           (json.dumps(receipt), row['id']))
        except Exception as exc:
            with self._connect() as db:
                db.execute("UPDATE imports SET state='failed',error=? WHERE id=?",
                           (type(exc).__name__ + ': ' + str(exc)[:500], row['id']))
        return self.get(row['id'])
