import yaml
import re

from lightkube.core import resource as lkr
from lightkube.resources.rbac_authorization_v1 import ClusterRole, Role
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.models.rbac_v1 import PolicyRule


from ..resources import resources_to_yaml


_rbac_registry = {}


# // +kubebuilder:rbac:
# groups=infrastructure.cluster.x-k8s.io,
# resources=mailgunclusters,
# verbs=get;list;watch;create;update;patch;delete
#
# // +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=mailgunclusters/status,verbs=get;update;patch


_roles = {}

def resource_rbac(resource, subresources=None, verbs=None, resource_names=None,
    namespace=None, urls=None,
):
    info = lkr.api_info(resource)
    groups = [info.resource.group]
    resources = []
    if subresources:
        if isinstance(subresources, str):
            subresources = re.split('[,;]', subresources)
        for subresource in subresources:
            resource = f'{info.plural}/{subresource}'
            resources.append(resource)
    else:
        resources = [info.plural]
    return rbac(
        resources=resources,
        groups=groups,
        verbs=verbs,
        resource_names=resource_names,
        namespace=namespace,
        urls=urls,
    )


# https://book.kubebuilder.io/reference/markers/rbac.html
def rbac(resources=None, groups=None, verbs=None, resource_names=None,
    namespace=None, urls=None, subresource=None,
):

    role = _roles.get(namespace, None)
    if role is None:
        if namespace is None:
            role = ClusterRole(
                metadata=ObjectMeta(
                    name='chopf-manager-role',
                ),
                rules=[],
            )
        else:
            role = Role(
                metadata=ObjectMeta(
                    name=f'chopf-manager-role-{namespace}',
                    namespace=namespace,
                ),
                rules=[],
            )
        _roles[namespace] = role

    #print(f'{role} {id(role)}')
    #print(f'{role.rules}')

    # TODO: maybe better to just only accept lists
    if isinstance(groups, str):
        groups = re.split('[,;]', groups)
    if isinstance(verbs, str):
        verbs = re.split('[,;]', verbs)
    if isinstance(resource_names, str):
        resource_names = re.split('[,;]', resource_names)
    if isinstance(urls, str):
        urls = re.split('[,;]', urls)

    rule = None
    for existing_rule in role.rules:
        if all((
            (existing_rule.verbs == verbs),
            (existing_rule.apiGroups == groups),
            (existing_rule.nonResourceURLs == urls),
            (existing_rule.resourceNames == resource_names),
            (existing_rule.resources == resources),
        )):
            rule = existing_rule
            break
    if rule is None:
        # create new rule
        rule = PolicyRule(
            verbs=verbs,
            apiGroups=groups,
            nonResourceURLs=urls,
            resourceNames=resource_names,
            resources=resources,
        )
        role.rules.append(rule)


def print_rbac():
    roles = []
    name = 'chopf-manager-role'

    role = list(_roles.values())[0]
    #print(role.__class__)
    #print(type(role))

    # TODO: possible optimization: merge rules which grant then same access.
    #       e.g all rules that have the same 'verbs'
    resources_yaml = resources_to_yaml(*_roles.values())
    print(resources_yaml)

