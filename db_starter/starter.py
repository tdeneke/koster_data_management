import getpass
import os

# Delete previous database if exists
if os.path.exists("koster_lab.db"):
  os.remove("koster_lab.db")
else:
  print("There are no previous database versions")

#from init import init_db
#from static import static_setup
#from subjects_uploaded import remove_duplicates

def main():

    # Your user name and password for Zooniverse. 
    zoo_user = getpass.getpass('Enter your Zooniverse user')
    zoo_pass = getpass.getpass('Enter your Zooniverse password')
    
    init_db()
    static_setup()
    remove_duplicates(zoo_user, zoo_pass)
    
if __name__ == "__main__":
    zoo_user = getpass.getpass('Enter your Zooniverse user')
    zoo_pass = getpass.getpass('Enter your Zooniverse password')
    main()
