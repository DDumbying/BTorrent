Name:           btorrent
Version:        1.1.0
Release:        1%{?dist}
Summary:        Concurrent BitTorrent client with DHT and magnet link support

License:        MIT
URL:            https://alsullam.github.io/btorrent
Source0:        %{name}-%{version}.tar.gz

BuildRequires:  gcc
BuildRequires:  make
BuildRequires:  pkgconfig(libcurl)
Requires:       libcurl

%description
btorrent is a command-line BitTorrent client for Linux written by Saeed Hany.
It uses an epoll-based scheduler for concurrent peer connections.

Features: DHT peer discovery (BEP 5), magnet link support via BEP 9/10
metadata fetch, seeding (--seed), Peer Exchange (BEP 11), upload/download
rate limiting, resume from incomplete downloads, endgame mode, and file
locking to prevent simultaneous instances on the same output path.

Protocols implemented: BEP 3, 5, 9, 10, 11, 12, 15.

%prep
%autosetup

%build
%make_build VERSION=%{version} all

%check
%make_build test

%install
%make_install PREFIX=%{_prefix} VERSION=%{version}

%files
%license LICENSE
%doc README.md CHANGES.md
%{_bindir}/btorrent
%{_mandir}/man1/btorrent.1*

%changelog
* Sun Apr 13 2026 Saeed Hany <saeed@saeeedhany.github.io> - 1.0.2-1
- Initial release.
