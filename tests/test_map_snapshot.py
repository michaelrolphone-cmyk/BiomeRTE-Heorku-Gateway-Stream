import pathlib,sys,unittest
from aiohttp import web
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent/'mac'))
from map_snapshot import reader

class SnapshotTests(unittest.IsolatedAsyncioTestCase):
 async def test_dictionary_cursor_preserves_complete_pinned_pages(self):
  calls=[]
  class Client:
   def query(self,operation,inputs):
    calls.append(inputs);n=len(calls)
    return {'map':{'map_id':'m','revision':12},'entities':[{'entity_id':str(n)}],
     'next_cursor':{'after':'one','revision':12} if n==1 else None}
  value=await reader(Client())('m',12)
  self.assertTrue(value['complete']);self.assertEqual(2,len(value['data']['entities']))
  self.assertEqual({'after':'one','revision':12},calls[1]['cursor'])
  self.assertTrue(all(c['revision']==12 for c in calls))
 async def test_wrong_revision_and_repeated_cursor_fail_without_partial_snapshot(self):
  class Wrong:
   def query(self,*args):return {'map':{'map_id':'m','revision':13},'entities':[]}
  with self.assertRaises(web.HTTPConflict):await reader(Wrong())('m',12)
  class Repeated:
   def query(self,*args):return {'map':{'map_id':'m','revision':12},'entities':[],
    'next_cursor':{'after':'one'}}
  with self.assertRaises(web.HTTPConflict):await reader(Repeated())('m',12)

if __name__=='__main__':unittest.main()
