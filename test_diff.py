#!/usr/bin/env python3
"""
Compare two yum package list files and show differences.
Shows packages that are:
- Only in file 1 (missing from file 2)
- Only in file 2 (new in file 2)
- Present in both but with different versions
"""

import sys
import re
from typing import Dict, Tuple, Set


def parse_yum_list(filepath: str) -> Dict[str, Tuple[str, str]]:
    """
    Parse a yum list installed output file.
    
    Returns a dictionary with:
    - key: package name
    - value: tuple of (version, architecture)
    
    Example line formats:
    NetworkManager.x86_64              1.18.8-2.el7_9           @updates
    NetworkManager-libnm.x86_64        1.18.8-2.el7_9           @updates
    """
    packages = {}
    
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            
            # Skip empty lines and header lines
            if not line or line.startswith('Installed') or line.startswith('Loaded'):
                continue
            
            # Parse the line - typical format: package.arch version repo
            parts = line.split()
            if len(parts) >= 2:
                # Split package name and architecture
                pkg_full = parts[0]
                version = parts[1]
                
                # Extract package name (everything before last dot)
                if '.' in pkg_full:
                    pkg_name = '.'.join(pkg_full.split('.')[:-1])
                    arch = pkg_full.split('.')[-1]
                else:
                    pkg_name = pkg_full
                    arch = 'noarch'
                
                packages[pkg_name] = (version, arch)
    
    return packages


def compare_packages(file1: str, file2: str):
    """Compare two yum package list files and display differences."""
    
    print(f"Comparing package lists:")
    print(f"  File 1: {file1}")
    print(f"  File 2: {file2}")
    print("=" * 80)
    
    # Parse both files
    packages1 = parse_yum_list(file1)
    packages2 = parse_yum_list(file2)
    
    # Get package sets
    set1 = set(packages1.keys())
    set2 = set(packages2.keys())
    
    # Packages only in file 1 (missing from file 2)
    only_in_1 = set1 - set2
    
    # Packages only in file 2 (new in file 2)
    only_in_2 = set2 - set1
    
    # Packages in both files
    in_both = set1 & set2
    
    # Check for version differences in common packages
    version_diffs = []
    for pkg in sorted(in_both):
        ver1, arch1 = packages1[pkg]
        ver2, arch2 = packages2[pkg]
        if ver1 != ver2 or arch1 != arch2:
            version_diffs.append((pkg, ver1, arch1, ver2, arch2))
    
    # Display results
    print(f"\nSummary:")
    print(f"  Total packages in file 1: {len(packages1)}")
    print(f"  Total packages in file 2: {len(packages2)}")
    print(f"  Common packages: {len(in_both)}")
    print(f"  Only in file 1: {len(only_in_1)}")
    print(f"  Only in file 2: {len(only_in_2)}")
    print(f"  Version differences: {len(version_diffs)}")
    
    # Packages only in file 1
    if only_in_1:
        print("\n" + "=" * 80)
        print(f"PACKAGES ONLY IN FILE 1 (missing from file 2): {len(only_in_1)}")
        print("=" * 80)
        for pkg in sorted(only_in_1):
            ver, arch = packages1[pkg]
            print(f"  {pkg}.{arch:10s}  {ver}")
    
    # Packages only in file 2
    if only_in_2:
        print("\n" + "=" * 80)
        print(f"PACKAGES ONLY IN FILE 2 (new packages): {len(only_in_2)}")
        print("=" * 80)
        for pkg in sorted(only_in_2):
            ver, arch = packages2[pkg]
            print(f"  {pkg}.{arch:10s}  {ver}")
    
    # Version differences
    if version_diffs:
        print("\n" + "=" * 80)
        print(f"VERSION DIFFERENCES: {len(version_diffs)}")
        print("=" * 80)
        print(f"{'Package':<40s} {'File 1':<25s} {'File 2':<25s}")
        print("-" * 80)
        for pkg, ver1, arch1, ver2, arch2 in version_diffs:
            pkg_display = f"{pkg}.{arch1}"
            ver1_display = ver1
            ver2_display = f"{ver2} ({arch2})" if arch1 != arch2 else ver2
            print(f"{pkg_display:<40s} {ver1_display:<25s} {ver2_display:<25s}")
    
    if not only_in_1 and not only_in_2 and not version_diffs:
        print("\n✓ Files are identical - no differences found!")


def main():
    if len(sys.argv) != 3:
        print("Usage: python compare_yum_packages.py <file1> <file2>")
        print("\nExample:")
        print("  python compare_yum_packages.py server1_packages.txt server2_packages.txt")
        sys.exit(1)
    
    file1 = sys.argv[1]
    file2 = sys.argv[2]
    
    try:
        compare_packages(file1, file2)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
