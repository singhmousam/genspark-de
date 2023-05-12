Directions to connect to local postgreSQL server:

# In the Airflow header go to Admin>Connection
# Create new connection using these parameters

Connection Type = Postgres
# inorder for docker ran in airflow to connect to localhost use host.docker.internal
Host = host.docker.internal
Schema = DE # Your schema
Login = postgres
Password = password

--------------------------------------------------------------------------------------------

Directions to create variables 
Admin>Variables

BUCKET_NAME = <aws bucket name>
LOCAL_PATH = C:\\Users\\<user>\\airflow_dags\\plugins\\user_purchase.csv
aws_default = aws://
----------------------------------------------------------------------------------------

Since this is an airflow run from a docker in Windows 10. 
Change the permission to allow all for the plugins folder.
Or postgres will not be able to write to it.

1. Go to Properties of that particular file by right clicking on it.
2. Then, go to Security tab of the displayed Properties dialog box. 
3. Click on Edit option.
4. Permissions dialog box appears, then click on Add button. 
5. Type 'Everyone' (without apostrophes) in the "Enter the object names to select" description box 
    and click on OK button. 
6. Then, make sure all the checkboxes of "Permissions for Everyone" are selected by 
    just ticking the "Full Control" check box to allow the control access without any restriction.
7. Then, Apply and OK all the tabs to apply all the changes done.