"""Resolve new Rover work using admitted, resource-specific controller offers."""
import json
import os
from contextlib import closing
from pathlib import Path
from urllib.error import HTTPError
import sqlite3


class ResourceMoved(ConnectionError):
    # Rover.maintain_lane recognizes a conflict and starts a fresh owner lifecycle.
    status = 409


def controller_candidates(local):
    default = Path(__file__).resolve().parents[1] / 'controllers.json'
    path = Path(os.getenv('ROVER_CONTROLLERS_FILE') or default)
    with path.open() as handle:
        members = json.load(handle)['authorities']
    if not isinstance(members, list) or any(not isinstance(v, str) or len(v) != 64
            or any(c not in '0123456789abcdef' for c in v) for v in members):
        raise ValueError('Invalid trusted controller inventory')
    return tuple(dict.fromkeys([local] + members))


class NoOffer(ConnectionError):
    def __init__(self, message, restricted):
        super().__init__(message)
        self.restricted = restricted


def resolve_offer(controllers, call, contract, resource):
    errors = []
    restricted = []
    for controller in controllers:
        try:
            catalog = call(controller, 'cap.discover', {'contract': contract})
        except OSError as error:
            if isinstance(error, HTTPError) and error.code == 403:
                restricted.append(controller)
            errors.append('{}: {}'.format(controller[:12], str(error)))
            continue
        for entry in catalog['contracts']:
            if entry['id'] != contract:
                continue
            for offer in entry['implementations']:
                if (offer.get('resource') == resource and offer.get('state') == 'ready'
                        and offer.get('admitted') is True):
                    return controller
    detail = ' ({})'.format(', '.join(errors)) if errors else ''
    raise NoOffer('No admitted offer for {} on {}{}'.format(contract, resource, detail), restricted)


class PendingSubmissions:
    """Remember the destination before sending, including across a lost reply or restart."""
    def __init__(self, path):
        self.path = str(path)
        with closing(self.connect()) as db, db:
            db.execute('CREATE TABLE IF NOT EXISTS pending (operation TEXT PRIMARY KEY, owner TEXT NOT NULL)')

    def connect(self):
        return sqlite3.connect(self.path)

    def owner(self, operation):
        with closing(self.connect()) as db, db:
            row = db.execute('SELECT owner FROM pending WHERE operation=?', (operation,)).fetchone()
        return row[0] if row else None

    def bind(self, operation, owner):
        with closing(self.connect()) as db, db:
            inserted = db.execute('INSERT OR IGNORE INTO pending VALUES (?,?)', (operation, owner))
            row = db.execute('SELECT owner FROM pending WHERE operation=?', (operation,)).fetchone()
            return row[0], inserted.rowcount == 1

    def settled(self, operation):
        with closing(self.connect()) as db, db:
            db.execute('DELETE FROM pending WHERE operation=?', (operation,))


class ControllerRoutes:
    def __init__(self, local, home, request):
        self.request = request
        self.controllers = controller_candidates(local)
        self.operations = PendingSubmissions(home / 'pending-routes.sqlite')
        self.leases = {}
        self.sessions = {}
        self.lease_resources = {}
        self.session_resources = {}

    def resolve(self, contract, resource):
        return resolve_offer(self.controllers, self.request, contract, resource)

    def call(self, operation, arguments):
        lease = arguments.get('lease')
        session = arguments.get('session')
        command = arguments.get('operation')
        if operation in ('cap.acquire', 'cap.submit'):
            owner = self.operations.owner(command) if operation == 'cap.submit' else None
            first_submission = False
            if owner is None:
                try:
                    owner = self.resolve(arguments['contract'], arguments['resource'])
                except NoOffer as error:
                    # Older paired callers may acquire an exact resource but lack catalog access.
                    # Ask a single such issuer for the real lease. Its native admission and
                    # device fence remain authoritative. Never fan out a mutating request.
                    if operation != 'cap.acquire' or len(error.restricted) != 1:
                        raise
                    owner = error.restricted[0]
                    arguments = dict(arguments, enqueue=False)
                if operation == 'cap.submit':
                    owner, first_submission = self.operations.bind(command, owner)
        elif operation == 'cap.status':
            owner = self.operations.owner(command)
            if owner is None:
                raise ValueError('Unknown Rover operation')
        elif lease is not None:
            owner = self.leases[lease]
        elif session is not None:
            owner = self.sessions[session]
        else:
            raise ValueError('Unroutable Rover operation')

        try:
            result = self.request(owner, operation, arguments)
        except OSError as error:
            if operation == 'cap.submit' and isinstance(error, HTTPError) and error.code in (400, 401, 403, 404, 422):
                # Only a definitive rejection of a first submission can clear its destination.
                if first_submission:
                    self.operations.settled(command)
            binding = self.session_resources.get(session) or self.lease_resources.get(lease)
            if binding and operation in ('stream.attach', 'cap.lease', 'stream.open'):
                try:
                    current = self.resolve(*binding)
                except OSError:
                    raise error
                if current != owner:
                    raise ResourceMoved('Resource now offered by another controller') from error
            raise

        if operation == 'cap.acquire':
            self.leases[result['id']] = owner
            self.lease_resources[result['id']] = (arguments['contract'], arguments['resource'])
        elif operation in ('stream.open', 'stream.attach'):
            self.sessions[result['id']] = owner
            if lease in self.lease_resources:
                self.session_resources[result['id']] = self.lease_resources[lease]
        elif operation == 'stream.list':
            self.sessions.update({item['id']:owner for item in result})
        elif operation == 'stream.close':
            self.sessions.pop(session, None)
            self.session_resources.pop(session, None)
        elif operation == 'cap.release' and result.get('state') == 'released':
            self.leases.pop(lease, None)
            self.lease_resources.pop(lease, None)
        elif operation == 'cap.status' and result.get('state') in (
                'succeeded', 'failed', 'abandoned', 'denied', 'cancelled'):
            self.operations.settled(command)
        return result
