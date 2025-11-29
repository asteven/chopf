import chopf

from lightkube.resources.core_v1 import Pod


# Get the store for Pods.
pod_store = chopf.store(Pod)


# Register a custom indexer function.
@pod_store.index('by_ip')
def index_by_ip(obj):
    ips = []
    try:
        pod_ips = obj.status.podIPs
        if pod_ips:
            ips = [item.ip for item in pod_ips]
    except AttributeError as e:
        print(e)
        pass
    finally:
        return ips


if __name__ == '__main__':
    chopf.run()

