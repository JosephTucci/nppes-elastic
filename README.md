# nppes-elastic

This copy is a clone of tosman's nppes-elastic minus the docker file. It only includes the python script to upload the nppes information to 
elastic cluster. Tested on python3.

#Steps to run nppes elasticsearch python script in windows

#powershell add python to path
[ENVIRONMENT]::SETENVIRONMENTVARIABLE("PATH", "$ENV:PATH;C:\Users\gsrinivasan\AppData\Local\Programs\Python\Python36-32", "USER")

# cmd line: add python to path
setx /M PATH "%PATH%;C:\Users\gsrinivasan\AppData\Local\Programs\Python\Python36-32"

#install pip and setup tools
python -m pip install -U pip setuptools

#install python pacakages
pip elasticsearch
