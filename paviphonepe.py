import git
import os
import shutil
def cloneRepo():
    #os.chmod('Phonepe_Pulse', 0o777)
    #shutil.rmtree('Phonepe_Pulse')
    git.Repo.clone_from('https://github.com/PhonePe/pulse.git','Phonepe_Pulse')
    
#cloneRepo()
