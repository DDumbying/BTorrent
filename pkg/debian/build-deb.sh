#!/bin/sh
# Build a binary .deb from a `make dist` tarball.
#
#   pkg/debian/build-deb.sh btorrent-X.Y.Z.tar.gz <output-dir>
#
# Used by CI and the release workflow. Needs: build-essential debhelper
# devscripts libcurl4-openssl-dev pkg-config.
set -eu

tarball=$(realpath "$1")
outdir=$(realpath -m "$2")
version=$(basename "$tarball" .tar.gz)
version=${version#btorrent-}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

tar -xzf "$tarball" -C "$work"
src="$work/btorrent-$version"
cp -r "$src/pkg/debian/debian" "$src/debian"
cd "$src"

# Keep debian/changelog in step with the tarball version, so tagging a
# release does not also require a hand-edited changelog entry.
if [ "$(dpkg-parsechangelog -S Version)" != "$version-1" ]; then
    DEBEMAIL=${DEBEMAIL:-$(sed -n 's/^Maintainer: //p' debian/control)} \
        dch --newversion "$version-1" --distribution unstable \
            "New upstream release $version."
fi

dpkg-buildpackage -b -us -uc
mkdir -p "$outdir"
mv "$work"/*.deb "$outdir"/
ls -l "$outdir"/*.deb
