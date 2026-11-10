import time
import unittest
from mac.site_bridge import SiteBridge

class Rover:
    control_ready=True
    descriptors={}
    def __init__(self):self.sent=[]
    def discard_intent(self):pass
    def desired(self,mask,ttl):
        self.sent.append((mask,ttl))
        return {'accepted_sequence':len(self.sent),'received_or_applied':False}

class IntentPolicy(unittest.TestCase):
    def bridge(self,ttl=500):
        rover=Rover()
        bridge=SiteBridge(rover,'x'*32,{'forward':1,'reverse':2,'left':3,'right':4},intent_ttl_ms=ttl)
        bridge.synchronize()
        return rover,bridge

    def test_stop_and_direction_use_same_server_budget(self):
        rover,bridge=self.bridge()
        message={'type':'intent','sequence':1,'epoch':bridge.epoch,
                 'stop_generation':bridge.stop_generation,'issued_at':time.time(),
                 'directions':{'forward':True,'reverse':False,'left':False,'right':False}}
        bridge.accept(message,0)
        bridge.stop()
        self.assertEqual([(1,500),(0,500)],rover.sent)
        self.assertEqual(500,bridge.state()['intent_ttl_ms'])

    def test_aligned_transport_can_configure_shorter_budget(self):
        rover,bridge=self.bridge(250)
        bridge.stop()
        self.assertEqual([(0,250)],rover.sent)

    def test_invalid_budget_cannot_exceed_firmware_bound(self):
        for ttl in [True,0,19,1001,500.0,'500']:
            with self.subTest(ttl=ttl),self.assertRaises(ValueError):self.bridge(ttl)
        self.bridge(20);self.bridge(1000)
