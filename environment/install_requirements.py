import subprocess


def install_from_lockfile(lockfile_path):
    with open(lockfile_path, 'r') as f:
        packages = f.readlines()

    for package in packages:
        package = package.strip()
        if package and not package.startswith('#'):
            subprocess.run(['pip', 'install', package])


if __name__ == "__main__":
    lockfile_path = "requirements.lock"
    install_from_lockfile(lockfile_path)
