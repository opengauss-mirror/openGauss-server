# encoding: utf-8
"""
This script is used to download the built Docker images from the artifact repository,
push them to Docker Hub, and create manifests to facilitate user downloads.
"""

import argparse
import logging
import os
import subprocess
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import docker
import requests
import urllib3
from bs4 import BeautifulSoup
from docker.errors import DockerException, ImageNotFound
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

# Base URL for package downloads
BASE_URL = "https://download-opengauss.osinfra.cn/archive_test/"
VERIFY = True
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Docker client
try:
    docker_client = docker.from_env()
    logger.info("Docker client initialized successfully")
except DockerException as e1:
    logger.error(f"Failed to initialize Docker client: {e1}")
    docker_client = None


# Create a session with retry capabilities
def create_session():
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    # Only disable ssl verify when using proxy
    session.verify = VERIFY
    return session


def select_spec_by_date(url, skip_choice=False):
    """Select version based on date with improved error handling."""
    session = create_session()
    try:
        response = session.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a')

        versions = []
        for link in links:
            href = link.get('href')
            if href and href.endswith('/') and href != '../':
                next_sibling = link.next_sibling
                while next_sibling and not isinstance(next_sibling, str):
                    next_sibling = next_sibling.next_sibling

                if next_sibling:
                    parts = next_sibling.strip().split()
                    if len(parts) >= 2:
                        date_str, time_str = parts[0], parts[1]
                        try:
                            date_time = datetime.strptime(f"{date_str} {time_str}", "%d-%b-%Y %H:%M")
                            versions.append((href[:-1], date_time))
                        except ValueError:
                            logger.warning(f"Failed to parse date: {date_str} {time_str}")

        if not versions:
            logger.warning(f"No versions found at {url}")
            return "", ""

        # Sort by time
        versions.sort(key=lambda x: x[1], reverse=True)

        if len(versions) == 1 or skip_choice:
            version = versions[0][0]
            version_url = f"{url}{version}/"
            return version, version_url

        print("Please select a version:")
        for i, (version, date_time) in enumerate(versions, start=1):
            print(f"{i}. {version} ({date_time.strftime('%Y-%m-%d %H:%M')})")

        choice = input(f"Enter a number from 1 to {len(versions)} to select a version, or press Enter for latest: ")

        if not choice:
            version = versions[0][0]
        else:
            try:
                index = int(choice) - 1
                if 0 <= index < len(versions):
                    version = versions[index][0]
                else:
                    logger.warning(f"Invalid selection index {index + 1}, using latest version")
                    version = versions[0][0]
            except ValueError:
                logger.warning("Invalid selection format, using latest version")
                version = versions[0][0]

        version_url = f"{url}{version}/"
        return version, version_url

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error while fetching versions: {e}")
        return "", ""


def find_docker_images(minor_version, minor_version_url, target_os=None, target_arch=None, get_all=False, image_version='both'):
    """Find Docker images with sequential OS directory processing."""
    images = []
    session = create_session()

    try:
        # Get OS directories
        response = session.get(minor_version_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        os_links = [link.get('href') for link in soup.find_all('a')
                    if link.get('href') and link.get('href').endswith('/') and link.get('href') != '../']

        # Process OS directories sequentially
        for os_link in os_links:
            try:
                os_images = process_os_directory(session, minor_version, minor_version_url, os_link, target_os,
                                                 target_arch, get_all, image_version)
                images.extend(os_images)
            except Exception as e:
                logger.error(f"Error processing OS directory {os_link}: {e}")

        return images

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error while finding Docker images: {e}")
        return []


def process_os_directory(session, minor_version, minor_version_url, os_link, target_os, target_arch, get_all, image_version):
    """Process a single OS directory to find Docker images."""
    images = []
    os_url = f"{minor_version_url}{os_link}"
    os_info = os_link[:-1]

    # Skip if we have a target OS and this isn't it
    if target_os is not None and os_info != target_os and not get_all:
        return images

    try:
        os_response = session.get(os_url)
        os_response.raise_for_status()
        os_soup = BeautifulSoup(os_response.text, 'html.parser')
        arch_links = [link.get('href') for link in os_soup.find_all('a')
                      if link.get('href') and link.get('href').endswith('/') and link.get('href') != '../']

        for arch_link in arch_links:
            arch_url = f"{os_url}{arch_link}"
            arch_info = arch_link[:-1]

            # Skip if we have a target arch and this isn't it
            if target_arch is not None and arch_info != target_arch and not get_all:
                continue

            try:
                arch_response = session.get(arch_url)
                arch_response.raise_for_status()
                arch_soup = BeautifulSoup(arch_response.text, 'html.parser')

                for link in arch_soup.find_all('a'):
                    href = link.get('href')
                    if href and href.endswith('.tar') and 'Docker' in href:
                        image_url = f"{arch_url}{href}"

                        # Get file size
                        file_size_str = link.next_sibling.strip().split()[-1] if link.next_sibling else '0'
                        try:
                            file_size = int(file_size_str)
                        except ValueError:
                            file_size = 0

                        images.append({
                            "os": os_info,
                            "arch": get_normalized_arch(arch_info),
                            "url": image_url,
                            "minor_version": minor_version.replace("openGauss", ""),
                            "file_size": file_size,
                            "filename": href,
                            "version": "lite" if 'openGauss-Lite-Docker' in href else "server"
                        })

            except requests.exceptions.RequestException as e:
                logger.error(f"Error processing architecture {arch_info}: {e}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error processing OS {os_info}: {e}")

    return images


def download_and_checksum(spec_info, save_path):
    """Download with resume capability and progress tracking."""
    url = spec_info["url"]
    minor_version = spec_info["minor_version"]
    arch = spec_info["arch"]
    os_version = spec_info["os"]
    version = spec_info["version"]
    file_name = f"{minor_version}-{version}-{arch}-{os_version}.tar"
    file_path = os.path.join(save_path, file_name)
    file_size = spec_info["file_size"]

    # Create directory if it doesn't exist
    Path(save_path).mkdir(parents=True, exist_ok=True)

    # Check if file exists and has correct size
    if os.path.exists(file_path):
        current_size = os.path.getsize(file_path)
        if current_size == file_size:
            logger.info(f"File {file_name} already downloaded successfully. Skipping.")
            return file_path
        elif file_size > 0:
            logger.warning(f"File {file_name} incomplete ({current_size}/{file_size} bytes). Redownloading.")
            os.remove(file_path)

    session = create_session()
    try:
        # Stream download with progress bar
        logger.info(f"Downloading {file_name}...")
        response = session.get(url, stream=True)
        response.raise_for_status()

        total_size = file_size if file_size > 0 else int(response.headers.get('content-length', 0))
        block_size = 1024 * 1024  # 1MB chunks for better performance

        with tqdm(total=total_size, unit='B', unit_scale=True, desc=file_name) as progress_bar:
            with open(file_path, 'wb') as f:
                for data in response.iter_content(block_size):
                    progress_bar.update(len(data))
                    f.write(data)

        # Verify download size
        downloaded_size = os.path.getsize(file_path)
        if 0 < total_size != downloaded_size:
            logger.error(f"Download incomplete: {downloaded_size}/{total_size} bytes")
            return None

        logger.info(f"Successfully downloaded {file_name}")
        return file_path

    except requests.exceptions.RequestException as e:
        logger.error(f"Download error: {e}")
        # Remove partial download
        if os.path.exists(file_path):
            os.remove(file_path)
        return None
    except Exception as e:
        logger.error(f"Unexpected error during download: {e}")
        return None


def get_normalized_arch(arch):
    """Convert architecture names to Docker standard format."""
    if arch.lower() in ['x86', 'x86_64', 'amd64']:
        return 'amd64'
    elif arch.lower() in ['arm', 'arm64', 'aarch64']:
        return 'arm64'
    else:
        return arch  # return as-is if not recognized


def load_docker_image(spec_info, file_path, namespace=None):
    """Load Docker image using the Docker Python SDK with normalized architecture names."""
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return False

    if docker_client is None:
        logger.error("Docker client not available")
        return False

    minor_version = spec_info["minor_version"]
    arch = get_normalized_arch(spec_info["arch"])
    os_name = spec_info["os"]
    if namespace is None:
        if spec_info.get("image_type") == "regular":
            namespace = 'opengauss/opengauss-server'
        else:  # lite or default
            namespace = 'opengauss/opengauss'
    tag = f"{namespace}:{minor_version}-{arch}-{os_name}"

    try:
        logger.info(f"Loading Docker image from {file_path}...")
        with open(file_path, 'rb') as image_file:
            image = docker_client.images.load(image_file.read())[0]

        # Get image ID for tagging
        image_id = image.id
        logger.info(f"Image loaded with ID: {image_id}")

        # Tag the image with proper architecture name
        logger.info(f"Tagging Docker image as {tag}...")
        repo, tag_part = tag.split(':')
        image.tag(repo, tag_part)

        # Verify image architecture
        inspect_result = docker_client.api.inspect_image(tag)
        image_arch = inspect_result.get('Architecture', '')

        # Map Docker's architecture names to our normalized ones for comparison
        docker_arch_map = {'amd64': 'amd64', 'arm64': 'arm64', 'x86_64': 'amd64', 'aarch64': 'arm64'}
        expected_arch = arch.lower()
        actual_arch = docker_arch_map.get(image_arch.lower(), image_arch.lower())

        if actual_arch != expected_arch and not (
                actual_arch in docker_arch_map and docker_arch_map[actual_arch] == expected_arch):
            logger.warning(f"Image architecture mismatch! Expected: {expected_arch}, Got: {actual_arch}")
            logger.warning(f"Continuing anyway, but please verify the image is built correctly")
        else:
            logger.info(f"Verified image architecture: {actual_arch}")

        return True
    except docker.errors.DockerException as e:
        logger.error(f"Docker error while loading image: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error while loading image: {e}")
        return False


def check_docker_login():
    """Check if Docker is logged in to Docker Hub using the Docker Python SDK."""
    if docker_client is None:
        logger.error("Docker client not available")
        return False

    try:
        # Try to get authentication information
        auth_info = docker_client.info()

        docker_client.ping()

        if 'RegistryConfig' in auth_info and auth_info['RegistryConfig'].get('IndexConfigs'):
            logger.info("Already logged in to Docker Hub")
            return True

        logger.warning("Not logged in to Docker Hub. Please login.")
        print("Docker Hub credentials required:")
        username = input("Username: ")
        password = input("Password: ")

        docker_client.login(username=username, password=password)
        logger.info("Successfully logged in to Docker Hub")
        return True

    except docker.errors.APIError as e:
        if '401' in str(e):
            logger.warning("Docker Hub authentication required")
            try:
                print("Docker Hub credentials required:")
                username = input("Username: ")
                password = input("Password: ")

                docker_client.login(username=username, password=password)
                logger.info("Successfully logged in to Docker Hub")
                return True
            except docker.errors.APIError as login_error:
                logger.error(f"Docker login failed: {login_error}")
                return False
        else:
            logger.error(f"Docker API error: {e}")
            return False
    except Exception as e:
        logger.error(f"Unexpected error checking Docker login: {e}")
        return False


def push_docker_image(tag_name):
    """Push Docker image using the Docker Python SDK."""
    if docker_client is None:
        logger.error("Docker client not available")
        return False

    if not check_docker_login():
        return False

    try:
        logger.info(f"Pushing Docker image {tag_name}...")

        # Split the tag name into repository and tag
        repo, tag = tag_name.split(':')

        # Make sure the image is in local
        try:
            image = docker_client.images.get(tag_name)
        except ImageNotFound:
            logger.error(f"Image {tag_name} not found locally")
            return False

        # Push the image with progress reporting
        for line in docker_client.api.push(repo, tag, stream=True, decode=True):
            if 'progress' in line:
                print(f"\r{line.get('id', '')}: {line.get('status', '')} {line.get('progress', '')}", end='')
            elif 'status' in line:
                status = line.get('status', '')
                if 'id' in line:
                    print(f"\r{status}: {line['id']}")
                else:
                    print(f"\r{status}")

                # Check for completion status
                if 'complete' in status.lower() or 'finished' in status.lower():
                    print()  # Add a newline for better formatting

        logger.info(f"Successfully pushed {tag_name}")
        return True

    except docker.errors.DockerException as e:
        logger.error(f"Docker error while pushing image {tag_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error while pushing image {tag_name}: {e}")
        return False


def make_manifest(successful_images, namespace='opengauss/opengauss', dry_run=False, latest=False):
    """
    Create and push Docker manifests with multi-arch support.
    Generates three types of manifests:
    1. OS-specific manifests (e.g.: 7.0.0-RC2.B001-openEuler20.03)
    2. Version manifest (e.g.: 7.0.0-RC2.B001)
    3. Latest manifest (when requested and single OS exists)
    """

    def create_and_push_manifest(manifest_name, tags, context=""):
        """Helper to create/push a manifest with proper error handling"""
        full_context = f"{context} " if context else ""

        try:
            logger.info(f"{full_context}Creating manifest {manifest_name} with tags: {', '.join(tags)}")

            if not dry_run:
                # Cleanup existing manifest
                rm_success, _, _ = run_command(
                    f"docker manifest rm {manifest_name} 2>/dev/null || true",
                    f"{full_context}Cleaning existing manifest"
                )
                if not rm_success:
                    logger.warning(f"{full_context}Failed to clean manifest - proceeding anyway")

                # Create manifest
                create_cmd = f"docker manifest create {manifest_name} {' '.join(tags)}"
                create_success, stdout, stderr = run_command(
                    create_cmd,
                    f"{full_context}Creating manifest"
                )
                if not create_success:
                    logger.error(f"{full_context}Create failed: {stderr}")
                    return False

                # Push manifest
                logger.info(f"{full_context}Pushing manifest...")
                push_success, _, stderr = run_command(
                    f"docker manifest push {manifest_name}",
                    f"{full_context}Pushing manifest"
                )
                if not push_success:
                    logger.error(f"{full_context}Push failed: {stderr}")
                    return False

            logger.info(f"{full_context}Manifest processed successfully" +
                        (" (dry run)" if dry_run else ""))
            return True

        except Exception as e:
            logger.error(f"{full_context}Unexpected error: {str(e)}")
            return False

    # Validate docker login early
    if not dry_run and not check_docker_login():
        return False

    # Organize images and track OS types
    image_groups = defaultdict(list)
    os_types = set()
    for spec in successful_images:
        os_types.add(spec['os'])
        key = (spec['minor_version'], spec['os'])
        image_groups[key].append(
            f"{namespace}:{spec['minor_version']}-{get_normalized_arch(spec['arch'])}-{spec['os']}"
        )

    success = True
    # 1. Process OS-specific manifests
    for (version, os_name), tags in image_groups.items():
        manifest_name = f"{namespace}:{version}-{os_name}"
        if not create_and_push_manifest(manifest_name, tags, f"[OS: {os_name}]"):
            success = False

    # 2. Create unified manifests only when single OS exists
    if len(os_types) == 1:
        all_tags = [tag for tags in image_groups.values() for tag in tags]
        os_name = os_types.pop()
        minor_version = next(iter(image_groups))[0]

        # Version manifest
        version_manifest = f"{namespace}:{minor_version}"
        if not create_and_push_manifest(version_manifest, all_tags, "[Version]"):
            success = False

        # Latest manifest (if requested)
        if latest:
            latest_manifest = f"{namespace}:latest"
            if not create_and_push_manifest(latest_manifest, all_tags, "[Latest]"):
                success = False

    return success


def run_command(cmd, desc=None):
    """Run a shell command with proper logging and error handling."""
    if desc:
        logger.info(desc)

    try:
        process = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        # Capture and display output in real-time
        stdout, stderr = [], []
        while True:
            output_line = process.stdout.readline()
            if not output_line and process.poll() is not None:
                break
            if output_line:
                line = output_line.strip()
                logger.info(line)
                stdout.append(line)

        # Get remaining output
        remaining_stdout, remaining_stderr = process.communicate()
        if remaining_stdout:
            for line in remaining_stdout.strip().split('\n'):
                if line:
                    logger.info(line)
                    stdout.append(line)

        if remaining_stderr:
            for line in remaining_stderr.strip().split('\n'):
                if line:
                    logger.error(f"Error: {line}")
                    stderr.append(line)

        if process.returncode != 0:
            error_msg = '\n'.join(stderr) if stderr else f"Command failed with return code {process.returncode}"
            logger.error(f"Command failed: {cmd}")
            logger.error(error_msg)
            return False, '\n'.join(stdout), '\n'.join(stderr)

        return True, '\n'.join(stdout), '\n'.join(stderr)

    except Exception as e:
        logger.error(f"Failed to execute command: {cmd}")
        logger.error(f"Error: {e}")
        return False, "", str(e)


def main():
    """Main function with improved Docker SDK integration."""
    parser = argparse.ArgumentParser(
        description="脚本用于自动化从制品仓库下载 Docker 镜像，并将其推送到 Docker Hub。",
        epilog="注意:\n如果未提供 MAJOR_VERSION 和 MINOR_VERSION，脚本将以交互式方式让你选择。"
    )
    parser.add_argument('-M', '--major-version', type=str, help='大版本号 (e.g., 7.0.0-RC1)，若未提供，可通过命令行选择')
    parser.add_argument('-m', '--minor-version', type=str,
                        help='完整版本号 (e.g., openGauss7.0.0-RC1.B020)，若未提供，可通过命令行选择')
    parser.add_argument('-o', '--os', type=str, help='目标操作系统。若未提供，默认选择所有操作系统。')
    parser.add_argument('-a', '--arch', type=str, help='目标架构。若未提供，默认选择所有架构。')
    parser.add_argument('--dry-run', action='store_true', help='测试模式，不将镜像推送到 Docker Hub')
    parser.add_argument('--save-dir', type=str, default=os.getcwd(), help='下载目录 (默认: 当前目录)')
    parser.add_argument('--skip-choice', action='store_true', help='跳过交互式提示，使用最新版本')
    parser.add_argument('--namespace', type=str, help='Docker标签命名空间，不传入时，lite 对应 opengauss/opengauss, 极简版对应 opengauss/opengauss-server')
    parser.add_argument('--latest', action='store_true', help='是否标记 latest 标签，仅在每个正式发版时使用')

    args = parser.parse_args()

    # Check if Docker client is available
    if docker_client is None:
        logger.error("Docker client not available. Please make sure Docker is installed and running.")
        return 1

    # Select major version
    if args.major_version:
        base_url = f"{BASE_URL}{args.major_version}/"
        logger.info(f"Using specified major version: {args.major_version}")
    else:
        logger.info("Selecting major version...")
        major_version, major_version_url = select_spec_by_date(BASE_URL, args.skip_choice)
        if not major_version:
            logger.error("Failed to select major version. Exiting.")
            return 1
        base_url = major_version_url
        logger.info(f"Selected major version: {major_version}")

    # Select minor version
    if args.minor_version:
        minor_version = args.minor_version
        minor_version_url = f"{base_url}/{minor_version}/"
        logger.info(f"Using specified minor version: {minor_version}")
    else:
        logger.info("Selecting minor version...")
        minor_version, minor_version_url = select_spec_by_date(base_url, args.skip_choice)
        if not minor_version:
            logger.error("Failed to select minor version. Exiting.")
            return 1
        logger.info(f"Selected minor version: {minor_version}")

    # Find Docker images
    logger.info("Finding Docker images...")
    docker_list = find_docker_images(minor_version, minor_version_url, args.os, args.arch, args.skip_choice)
    if not docker_list:
        logger.error("No Docker images found. Exiting.")
        return 1

    logger.info(f"Found {len(docker_list)} Docker images")

    # Process each image
    successful_namespace = defaultdict(list)
    for spec in docker_list:
        logger.info(f"Processing image: {spec['minor_version']}-{spec['arch']}-{spec['os']}")

        # Download image
        file_path = download_and_checksum(spec, args.save_dir)
        if not file_path:
            logger.error(f"Failed to download image: {spec['minor_version']}-{spec['arch']}-{spec['os']}")
            continue
        namespace = args.namespace
        if not args.namespace:
            if spec['version'] == 'server':
                namespace = 'opengauss/opengauss-server'
            else:
                namespace = 'opengauss/opengauss'

        # Load image using Docker SDK
        if not load_docker_image(spec, file_path, namespace):
            logger.error(f"Failed to load image: {spec['minor_version']}-{spec['arch']}-{spec['os']}")
            continue

        tag_name = f"{namespace}:{spec['minor_version']}-{spec['arch']}-{spec['os']}"

        # Push image if not in dry-run mode
        if not args.dry_run:
            print(f"\nVersion to push: {spec['minor_version']}")
            print(f"OS: {spec['os']}")
            print(f"Architecture: {spec['arch']}")
            print(f"Size: {os.path.getsize(file_path):,} bytes")

            if args.skip_choice or input("Push this image? (y/n): ").lower() == 'y':
                if push_docker_image(tag_name):
                    successful_namespace[namespace].append(spec)
                    logger.info(f"Successfully pushed {tag_name}")
                else:
                    logger.error(f"Failed to push {tag_name}")
            else:
                logger.info(f"Skipping push for {tag_name}")
        else:
            logger.info(f"Dry run: would push {tag_name}")
            successful_namespace[namespace].append(spec)

    for namespace in successful_namespace:
        logger.info(f"Creating Docker manifests for {namespace}...")
        make_manifest(successful_namespace[namespace], namespace, args.dry_run, args.latest)

    logger.info("Script execution completed")
    if not args.skip_choice:
        input("Press any key to exit...")

    return 0


if __name__ == "__main__":
    # Apply proxy settings if available
    proxy = os.getenv('HTTP_PROXY') or os.getenv('HTTPS_PROXY')
    if proxy:
        os.environ['DOCKER_PROXY'] = proxy
        logger.info(f"Using proxy: {proxy}")
        # Disable SSL warnings
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        VERIFY = False
    sys.exit(main())
