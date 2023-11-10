import os

from dotenv import load_dotenv

from gitlab import GitLab

load_dotenv()

SOURCE_URL = os.getenv('SOURCE_URL', '')
SOURCE_USERNAME = os.getenv('SOURCE_USERNAME', '')
SOURCE_ACCESS_TOKEN = os.getenv('SOURCE_ACCESS_TOKEN', '')
SRC_REPO_EXCEPTIONS = os.getenv('SRC_REPO_EXCEPTIONS', []).split(',')

DEST_URL = os.getenv('DEST_URL', '')
DEST_USERNAME = os.getenv('DEST_USERNAME', '')
DEST_ACCESS_TOKEN = os.getenv('DEST_ACCESS_TOKEN', '')


def main():
    src_gitlab = GitLab(SOURCE_URL, SOURCE_ACCESS_TOKEN, SOURCE_USERNAME)
    dst_gitlab = GitLab(DEST_URL, DEST_ACCESS_TOKEN, DEST_USERNAME)

    # Create groups and repos in the dest GitLab with same structure as src GitLab under a separate group
    group = list(filter(lambda grp: grp.name == 'test', dst_gitlab.groups))[0]
    for tree in src_gitlab.trees:
        dst_gitlab.copy_tree(tree, group)

    # Refetch the groups and projects from GitLab
    dst_gitlab.refetch_data()

    # Clone the projects from src GitLab and upload them to dest GitLab
    src_gitlab.mirror_all_projects(SRC_REPO_EXCEPTIONS)
    dst_gitlab.upload_all_projects()

    # Remove the data from temp
    dst_gitlab.clean_temp_folder()

    # Clone all projects with mirroring the git structure
    dst_gitlab.mirror_all_projects(SRC_REPO_EXCEPTIONS)

    # Relink the submodules in the dst repositories (if group was used as root, needs to be provided here as well)
    dst_gitlab.relink_references(SOURCE_URL, group)

    # Remove the data from temp
    dst_gitlab.clean_temp_folder()


if __name__ == '__main__':
    main()
