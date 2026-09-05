# gpstream RPM spec (FR-05: package delivery).
# Built by packaging/rpm via rpmbuild. The static binary carries no external
# runtime dependency (FR-08: only free components, one static binary).
Name:        gpstream
Version:     0.1.0
Release:     1%{?dist}
Summary:     High-throughput logical data copy between Greenplum 6 clusters

License:     Apache-2.0
URL:         https://github.com/gpstream/gpstream
Source0:     gpstream-%{version}.tar.gz
# No runtime Requires: the Go static binary links only the C runtime it
# bundles; TLS/zlib are in-process, zstd is pure-Go (klauspost, BSD).
BuildRequires: go

%description
gpstream streams logical data between two Greenplum 6 clusters over a direct
high-speed network without intermediate storage, with snapshot-level
consistency, fault tolerance, auto-resume, monitoring, background index
rebuild, redistribution awareness and a detailed --dry-run.

%prep
%autosetup -n gpstream-%{version}

%build
export GOPROXY=https://proxy.golang.org,direct
# Build the static binary for the target architecture (cross-compiled in CI).
go build -o bin/gpstream ./cmd/gpstream

%install
install -d %{buildroot}%{_bindir}
install -d %{buildroot}%{unitdir}
install -d %{buildroot}%{_sysconfdir}/gpstream
install -d %{buildroot}%{_mandir}/man8
install bin/gpstream %{buildroot}%{_bindir}/gpstream
install packaging/systemd/gpstream.service %{buildroot}%{unitdir}/gpstream.service
install packaging/examples/gpstream.yaml %{buildroot}%{_sysconfdir}/gpstream/gpstream.yaml
install man/gpstream.8 %{buildroot}%{_mandir}/man8/gpstream.8

%post
%systemd_post gpstream.service

%preun
%systemd_preun gpstream.service

%postun
%systemd_postun gpstream.service

%files
%{_bindir}/gpstream
%unitdir/gpstream.service
%config(noreplace) %{_sysconfdir}/gpstream/gpstream.yaml
%{_mandir}/man8/gpstream.8

%changelog
* Mon Sep 01 2026 gpstream maintainers <gpstream@example.com> - 0.1.0-1
- Initial release: M0-M4 (doctor/selftest, job journal, stream, limiter,
  metrics, compress, resume/crash-recovery, index rebuild, dry-run, plan,
  status, cleanup, man, cross-compile amd64/arm64, packaging).
