"""Install the already-paired Rover server beside its native controller."""
import argparse,os,pathlib,plistlib,subprocess

def main():
 p=argparse.ArgumentParser()
 p.add_argument('--runtime',required=True,type=pathlib.Path)
 p.add_argument('--vision',required=True,type=pathlib.Path)
 p.add_argument('--python',required=True,type=pathlib.Path)
 a=p.parse_args();source=pathlib.Path(__file__).resolve().parent.parent
 for x in (a.runtime/'biomerte/rover.py',a.vision/'providers/robot-vision/biome_provider.py',a.python):
  if not x.exists():raise SystemExit('Required runtime input unavailable: '+str(x))
 home=pathlib.Path.home()/'.local/share/biome-rover-server';home.mkdir(mode=0o700,parents=True,exist_ok=True)
 if not (home/'node.key').exists():raise SystemExit('Enroll the paired Rover-server identity first')
 label='org.biomerte.rover-server';target=pathlib.Path.home()/'Library/LaunchAgents'/(label+'.plist')
 paths=[a.runtime,source/'mac',source/'biomerte',a.vision/'providers/robot-vision']
 value=dict(Label=label,ProgramArguments=[str(a.python),str(source/'mac/rover_service.py')],
  WorkingDirectory=str(source/'mac'),EnvironmentVariables={'HOME':str(pathlib.Path.home()),
  'PYTHONPATH':os.pathsep.join(map(str,paths))},RunAtLoad=True,KeepAlive=True,
  StandardOutPath=str(home/'service.log'),StandardErrorPath=str(home/'service-error.log'),Umask=63)
 target.write_bytes(plistlib.dumps(value));os.chmod(target,0o600)
 domain='gui/'+str(os.getuid())
 subprocess.run(['launchctl','bootout',domain,str(target)],capture_output=True)
 subprocess.run(['launchctl','bootstrap',domain,str(target)],check=True)
 print('Installed',label,'with runtime',a.runtime)

if __name__=='__main__':main()
