import re
import sys
import requests

def get_git_sha(repo, tag):
    url = f"https://api.github.com/repos/{repo}/git/refs/tags/{tag}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to get Git SHA for tag {tag}: {response.status_code}")
    data = response.json()
    return data['object']['sha'][:7]

def update_yaml_file(yaml_file, version):
    # Extract the minor version from the input version
    minor_version = version.split('.')[1]

    # Read the YAML file
    with open(yaml_file, 'r') as file:
        content = file.read()

    # Replace the line with the new minor version
    updated_content = re.sub(
        r'ref: release-x\.\d+\.x',
        f'ref: release-x.{minor_version}.x',
        content
    )
    updated_content = re.sub(
        r'metabase-greptimedb:v\d+\.\d+\.\d+(\.\d+)?',
        f'metabase-greptimedb:{version}',
        content
    )

    # Write the updated content back to the YAML file
    with open(yaml_file, 'w') as file:
        file.write(updated_content)

def update_dockerfile(dockerfile, version):
    # Read the Dockerfile
    with open(dockerfile, 'r') as file:
        content = file.read()

    # Replace the old version number with the new one
    updated_content = re.sub(
        r'v\d+\.\d+\.\d+(\.\d+)?',
        version,
        content
    )

    # Write the updated content back to the Dockerfile
    with open(dockerfile, 'w') as file:
        file.write(updated_content)

def update_deps_edn(deps_file, version, git_sha):
    # Read the last file
    with open(deps_file, 'r') as file:
        content = file.read()

    # Replace the old version number with the new one
    updated_content = re.sub(
        r'v\d+\.\d+\.\d+(\.\d+)?',
        version,
        content
    )

    # Replace the old Git SHA with the new one
    updated_content = re.sub(
        r'[a-f0-9]{7,40}',
        git_sha,
        updated_content
    )

    # Write the updated content back to the last file
    with open(deps_file, 'w') as file:
        file.write(updated_content)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python update_yaml.py <version>")
        sys.exit(1)

    version = sys.argv[1]

    yaml_files = ['.github/workflows/ci.yml', '.github/workflows/build-on-release.yml']
    for yaml_file in yaml_files:
        update_yaml_file(yaml_file, version)
    update_dockerfile('docker/Dockerfile', version)

    git_sha = get_git_sha('metabase/metabase', version)
    update_deps_edn('deps.edn', version, git_sha)

    print(f"Updated project with version {version}")
