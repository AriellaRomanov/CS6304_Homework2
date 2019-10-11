# HBase_Demo

### Importing Project:
* Open “eclipse”, right click on “Package Explorer” window, click import.
* Select “Git”-> “Projects from Git” and click “next”.
* Select “clone url” and click “next”.
* Paste “https://github.com/shudipdatta/HBase_Demo.git” in the “url” textbox, Change protocol to “git”, and click “next”. 
* Choose “Import existing project” and click “finish”.

### Referencing libraries:
* Right click on project and select “build path”-> “configure build path” ->”libraries”->”add external jars”.
* Go to the directory “File System/usr/lib/hadoop” and select all jars
* Go to the directory “File System/usr/lib/hbase” and select all jars
* Go to the directory “File System/usr/lib/hbase-solr/lib” and select all jars
* click ok

### General Information (Cloudera):

* Operating System:         Mac -> Microsoft Remote Desktop, Windows -> Default Remote Desktop, Ubuntu -> Remmina
* Machine:                  cqs-cs6304-xxx.ats.mst.edu
* User:                     cloudera
* Default Password:         stu-pass
* Change Password Command:  sudo passwd cloudera

* "Firefox already running" error solve by command:     
* killall -SIGTERM firefox
* "Eclipse workspace in use" error solve by command: 
* cd ~/yourWorkspaceDirectory/.metadata
* rm .lock
* If HBase doesn't work and shows the error: "Can't get master address from ZooKeeper; znode data == null"
* sudo /sbin/service hbase-master restart # On Master host
* sudo /sbin/service hbase-regionserver restart # On all RS hosts
