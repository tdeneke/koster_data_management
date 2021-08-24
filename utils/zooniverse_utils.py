from panoptes_client import (
    SubjectSet,
    Subject,
    Project,
    Panoptes,
)


class AuthenticationError(Exception):
    pass


def auth_session(username, password, project_n):
    # Connect to Zooniverse with your username and password
    auth = Panoptes.connect(username=username, password=password)

    if not auth.logged_in:
        raise AuthenticationError("Your credentials are invalid. Please try again.")

    # Specify the project number of the koster lab
    project = Project(project_n)

    return project    
