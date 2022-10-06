# ingestion-framework

TO create a wheel file, code base should contain setup.py file as is available here. Setup.py specifies all the dependent libraries needed by the application and to be packaged a part of wheel.

Once setup.py is available open command prompt and CD into the folder which contains setup.py and other application py files.
From command line execute below commands.

````
pip install wheel
python -m pip install --upgrade pip
pip install check-wheel-contents
python setup.py bdist_wheel 
````
Once done wheel(.whl) file would be available in dist folder created as part of the process.
