import asyncio
import concurrent.futures
import enum
import os
import stat
import uuid
import subprocess
from dataclasses import dataclass
import json
from typing import List, Optional, Tuple

import requests


class Visibility(enum.Enum):
    PRIVATE = 'private'
    INTERNAL = 'internal'
    PUBLIC = 'public'


@dataclass
class BaseResource:
    id: int
    name: str
    web_url: str
    path: str


@dataclass
class Namespace(BaseResource):
    kind: str
    full_path: str
    parent_id: int


@dataclass
class Project(BaseResource):
    description: str
    namespace: Optional[Namespace] = None
    remote: Optional[str] = ''


@dataclass
class Group(BaseResource):
    full_path: str
    description: str
    parent_id: int
    visibility: Visibility


@dataclass
class GroupNode:
    item: Group
    children: list


def rmtree(top):
    for root, dirs, files in os.walk(top, topdown=False):
        for name in files:
            filename = os.path.join(root, name)
            os.chmod(filename, stat.S_IWUSR)
            os.remove(filename)
        for name in dirs:
            os.rmdir(os.path.join(root, name))
    os.rmdir(top)


class GitLab:

    def __init__(self, url: str, access_token: str, username: str = '', temp_path: str = 'temp'):
        self.trees = []
        self.projects = []
        self.groups = []
        self.url = url
        self.username = username
        self.access_token = access_token
        self.temp_path = temp_path
        self.refetch_data()
        self.create_temp_folder()

    def create_temp_folder(self):
        if not os.path.exists(self.temp_path):
            os.makedirs(self.temp_path, exist_ok=True)

    def clean_temp_folder(self):
        if os.path.exists(self.temp_path):
            rmtree(self.temp_path)
        os.makedirs(self.temp_path, exist_ok=True)

    @staticmethod
    def _construct_trees_roots(groups: List[Group], projects: List[Project]) -> \
            Tuple[List[GroupNode], List[Group], List[Project]]:
        remaining_groups = []
        remaining_projects = []
        roots = []
        for group in groups:
            if not group.parent_id:
                root = GroupNode(group, [])
                roots.append(root)
            else:
                remaining_groups.append(group)
        for project in projects:
            if not project.namespace.id:
                roots.append(project)
            else:
                remaining_projects.append(project)
        return roots, remaining_groups, remaining_projects

    @staticmethod
    def _find_children(root: GroupNode, groups: List[Group], projects: List[Project]):
        group_children = list(filter(lambda grp: grp.parent_id == root.item.id, groups))

        for group in group_children:
            node = GroupNode(group, [])
            GitLab._find_children(node, groups, projects)
            root.children.append(node)

        root.children += list(filter(lambda prj: prj.namespace.id == root.item.id, projects))

    @staticmethod
    def _deserialize_group(group_data: dict) -> Group:
        return Group(
            id=group_data['id'],
            name=group_data['name'],
            description=group_data['description'],
            parent_id=group_data['parent_id'],
            visibility=Visibility(group_data['visibility']),
            web_url=group_data['web_url'],
            path=group_data['path'],
            full_path=group_data['full_path']
        )

    def create_group(self, group_name: str, description: str = '',
                     path: str = '', visibility: Visibility = Visibility.PRIVATE,
                     parent_id: int = -1) -> Group or None:
        params = {
            'private_token': self.access_token,
            'name': group_name,
            'path': path,
            'description': description,
            'visibility': visibility.value
        }
        if parent_id >= 0:
            params['parent_id'] = parent_id

        if not path:
            params['path'] = group_name.replace(' ', '-').replace('/', '-').replace('\\', '-')
        response = requests.post(f'{self.url}/api/v4/groups', params=params)

        if response.status_code == 201:
            group = self._deserialize_group(json.loads(response.text))
            self.groups.append(group)
            return group

        print(f'{group_name}, response: {response.text}')
        return None

    def fetch_groups(self, next_id: int = 0) -> List[Group]:
        per_page = 100
        params = {
            'private_token': self.access_token,
            'order_by': 'id',
            'sort': 'asc',
            'per_page': per_page
        }
        if next_id:
            params['id_after'] = next_id
        response = requests.get(f'{self.url}/api/v4/groups', params=params)
        if response.status_code != 200:
            return []

        data = json.loads(response.text)

        groups = []
        for group_data in data:
            groups.append(self._deserialize_group(group_data))

        if len(data) >= per_page:
            groups += self.fetch_groups(data[-1]['id'])

        return groups

    @staticmethod
    def _deserialize_namespace(namespace_data: dict) -> Namespace:
        return Namespace(
            id=namespace_data['id'],
            name=namespace_data['name'],
            kind=namespace_data['kind'],
            full_path=namespace_data['full_path'],
            parent_id=namespace_data['parent_id'],
            web_url=namespace_data['web_url'],
            path=namespace_data['path']
        )

    def _deserialize_project(self, proj_data: dict) -> Project:
        return Project(
            id=proj_data['id'],
            name=proj_data['name'],
            description=proj_data['description'],
            web_url=proj_data['web_url'],
            path=proj_data['path'],
            namespace=self._deserialize_namespace(proj_data['namespace']) if 'namespace' in proj_data else None
        )

    def create_project(self, prj_name: str, description: str = '',
                       path: str = '', visibility: Visibility = Visibility.PRIVATE,
                       parent_id: int = -1) -> Group or None:
        params = {
            'private_token': self.access_token,
            'name': prj_name,
            'path': path,
            'description': description,
            'visibility': visibility.value
        }

        if parent_id >= 0:
            params['namespace_id'] = parent_id

        if not path:
            params['path'] = prj_name.replace(' ', '-').replace('/', '-').replace('\\', '-')

        response = requests.post(f'{self.url}/api/v4/projects', params=params)

        if response.status_code == 201:
            project = self._deserialize_project(json.loads(response.text))
            self.projects.append(project)
            return project

        print(f'{prj_name}, response: {response.text}')
        return None

    def fetch_projects(self, next_id: int = 0) -> List[Project]:
        per_page = 100
        params = {
            'private_token': self.access_token,
            'order_by': 'id',
            'sort': 'asc',
            'per_page': per_page
        }
        if next_id:
            params['id_after'] = next_id
        response = requests.get(f'{self.url}/api/v4/projects', params=params)
        if response.status_code != 200:
            return []

        data = json.loads(response.text)

        projects = []
        for proj_data in data:
            projects.append(self._deserialize_project(proj_data))

        if len(data) >= per_page:
            projects += self.fetch_projects(data[-1]['id'])

        return projects

    def refetch_data(self):
        self.groups = self.fetch_groups()
        self.projects = self.fetch_projects()
        self.trees, rem_groups, rem_prjs = self._construct_trees_roots(self.groups, self.projects)
        for tree in self.trees:
            self._find_children(tree, rem_groups, rem_prjs)

    def copy_tree(self, tree: GroupNode, parent_group: Group = None):
        parent_id = -1
        if parent_group:
            parent_id = parent_group.id

        if isinstance(tree, GroupNode):
            root_group = self.create_group(tree.item.name, tree.item.description, visibility=Visibility.INTERNAL,
                                           path=tree.item.path, parent_id=parent_id)

            # Check if group exists since the group was not created
            if root_group is None:
                filtered_res = list(filter(lambda group: group.name == tree.item.name, self.groups))
                if filtered_res:
                    root_group = filtered_res[0]
        else:
            return

        for child in tree.children:
            if isinstance(child, Project):
                self.create_project(child.name, child.description, visibility=Visibility.INTERNAL,
                                    path=child.path, parent_id=root_group.id)
            else:
                self.copy_tree(child, root_group)

    def mirror_project(self, project: Project):
        if not self.username:
            print('Username is not specified for cloning')
            return
        console_args = ['git', 'clone', '--mirror']

        abs_path = os.path.abspath(self.temp_path)

        print(f'Cloning {project.name}')
        protocol, path = project.web_url.split('://')
        final_url = f'{protocol}://{self.username}:{self.access_token}@{path}.git'
        console_args.append(final_url)

        process = subprocess.Popen(console_args, stdout=subprocess.PIPE, cwd=abs_path)
        process.wait()

    def mirror_all_projects(self, exceptions: List[str]):
        loop = asyncio.new_event_loop()
        threadpool = concurrent.futures.ThreadPoolExecutor()

        projects = self.projects

        if exceptions:
            projects = list(filter(lambda prj: prj.name not in exceptions, projects))

        coroutines = []
        for project in projects:
            coroutine = loop.run_in_executor(threadpool, lambda prj=project: self.mirror_project(prj))
            coroutines.append(coroutine)

        loop.run_until_complete(asyncio.gather(*coroutines))
        loop.close()

    def upload_project(self, project: Project):
        if not self.username:
            print('Username is not specified for cloning')
            return

        abs_path = os.path.abspath(f'{self.temp_path}/{project.path}.git')

        if not os.path.exists(abs_path):
            print(f'Folder {project.name} does not exist in {self.temp_path}')
            return

        if not project.remote:
            project.remote = str(uuid.uuid4())

        protocol, path = project.web_url.split('://')
        final_url = f'{protocol}://{self.username}:{self.access_token}@{path}.git'
        process = subprocess.Popen(['git', 'remote', 'add', project.remote, final_url],
                                   stdout=subprocess.PIPE, cwd=abs_path)
        process.wait()

        process = subprocess.Popen(['git', 'push', project.remote, '--mirror'],
                                   stdout=subprocess.PIPE, cwd=abs_path)
        process.wait()

    def upload_all_projects(self):
        loop = asyncio.new_event_loop()
        threadpool = concurrent.futures.ThreadPoolExecutor()

        coroutines = []
        for project in self.projects:
            coroutine = loop.run_in_executor(threadpool, lambda prj=project: self.upload_project(prj))
            coroutines.append(coroutine)

        loop.run_until_complete(asyncio.gather(*coroutines))
        loop.close()

    @staticmethod
    def remove_cloned_repo(project: Project):
        if not os.path.exists(project.name):
            return

        abs_path = os.path.abspath(project.name)
        rmtree(abs_path)

    def _create_links_replacement_file(self, src_url: str, dst_url: str):
        replacement_str = f'{src_url}==>{dst_url}\r\n'
        replacement_str += f'@{src_url.split("://")[1]}==>@{dst_url.split("://")[1]}\r\n'
        with open(f'{self.temp_path}/replace.txt', 'w') as f:
            f.write(replacement_str)

    def _relink_project(self, project: Project):
        if not self.username:
            print('Username is not specified for cloning')
            return

        abs_path = os.path.abspath(f'{self.temp_path}/{project.name}.git')

        if not os.path.exists(abs_path):
            print(f'Folder {project.name}.git does not exist in {self.temp_path}')
            return

        # Replace text across all the commits
        process = subprocess.Popen(['git-filter-repo.exe', '--replace-text', '../replace.txt'],
                                   stdout=subprocess.PIPE, cwd=abs_path)
        process.wait()

        # Push the changed commits
        process = subprocess.Popen(['git', 'push', '--force'], stdout=subprocess.PIPE, cwd=abs_path)
        process.wait()

    def relink_references(self, src_url: str, dst_group: Group = None):
        if dst_group:
            dst_url = f'{self.url}/{dst_group.full_path}'
        else:
            dst_url = self.url

        self._create_links_replacement_file(src_url, dst_url)

        loop = asyncio.new_event_loop()
        threadpool = concurrent.futures.ThreadPoolExecutor()

        coroutines = []
        for project in self.projects:
            coroutine = loop.run_in_executor(threadpool, lambda prj=project: self._relink_project(prj))
            coroutines.append(coroutine)

        loop.run_until_complete(asyncio.gather(*coroutines))
        loop.close()
