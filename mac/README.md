# Paired Mac Rover service

The public Heroku router also provides authenticated reverse ingress for the Mac-owned Rover server. Native stream sessions remain on the Heroku router; resource ownership and browser input handling stay in the long-running Mac service. Video, map and control use separate connections. Neither incoming Mac ports nor a second public tunnel is required.

`site_bridge.py` is the existing authenticated adapter from Rover Site source `2884107366bb133c56c041bf9c8671b6366b2fc8`. The native runtime supplies `biomerte.rover.Rover`; the existing vision service supplies frame feeds and revision-pinned map reads. Private keys stay on the Mac. `rover_service.py` signs calls as the separately enrolled Rover-server identity and selects the Mac or Pi authority explicitly. It never sends motion during startup or browser attachment.

Before activation, enroll that identity on both controllers for cap acquire/read/release and stream open/attach/list/inspect/close only. Migrate the existing discrete server lease after confirming its active commands have settled and neutral output is confirmed. Physical direction mapping is the existing deployed configuration: forward1, reverse2, left3, right4.

Install with the existing vision environment interpreter:

```
python mac/install_service.py --runtime /absolute/native/release --vision /absolute/vision/release --python /absolute/vision/env/bin/python
```

Both runtime and working-directory imports must refer to the intended release. Keep the virtual-environment interpreter path without resolving its symlink. The controller's existing owner-only stream secret derives separate reverse-link and Site transport credentials; no secret is saved in source or printed by this installer. Set the Rover Site HTTPS origin to the Heroku origin and its private `ROVER_STREAM_TOKEN` to the HMAC-SHA256 hex digest of `rover-site` under the shared secret. Redeploy the Site to apply those variables.

The service drops frames older than500ms since processing using the Mac monotonic clock shared by provider and server. This does not establish camera capture age. Browser detach leaves resource leases held; explicit service shutdown closes them. Reverse-link loss closes public lanes but never replays input or ends the Mac owner process. Each control intent expires at the relay independently of connection heartbeats.
