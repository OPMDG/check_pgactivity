%global _tag REL2_0

Name: nagios-plugins-pgactivity
Version: 2.0
Release: 1
Summary: PostgreSQL monitoring plugin for Nagios
License: PostgreSQL
Group: Applications/Databases
Url: https://github.com/OPMDG/check_pgactivity

Source0: https://github.com/OPMDG/check_pgactivity/archive/%{_tag}.tar.gz
BuildArch: noarch
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
Requires: postgresql
Requires: nagios-plugins
Provides: check_pgactivity = %{version}

%description
check_pgactivity is a monitoring plugin of PostgreSQL for Nagios. It provides
many checks and allow the gathering of many performance counters.
check_pgactivity is part of Open PostgreSQL Monitoring.

%prep
%setup -n check_pgactivity-%{_tag}

%install
install -D -p -m 0755 check_pgactivity %{buildroot}/%{_libdir}/nagios/plugins/check_pgactivity

%files
%defattr(-,root,root,0755)
%{_libdir}/nagios/plugins/check_pgactivity
%doc README.rst LICENSE

%changelog
* Mon Aug 29 2016 Jehan-Guillaume de Rorthais <jgdr@dalibo.com> 2.0-1
- new major release 2.0
- support various output format
- add output format "nagios_strict"
- add output format "debug"
- add output format "binary"
- add output format "human"
- force UTF8 encoding
- do not connect ot the cluster if using --dbinclude for service pg_dump_backup
- add argument --dump-status-file, useful for debugging
- add service "table_unlogged"
- add basic support to timeline cross in service archive_folder
- add service "settings"
- add service "invalid_indexes"
- fix a bug where pod2usage couldn't find the original script
- fix wal size computation for 9.3+ (255 -vs- 256 seg of 16MB)
- fix perl warning with pg_dump_backup related to unknown database
- fix buffers_backend unit in check_bgwriter

* Thu Jan 28 2016 Jehan-Guillaume de Rorthais <jgdr@dalibo.com> 1.25-1
- update to release 1.25

* Tue Jan 05 2016 Jehan-Guillaume de Rorthais <jgdr@dalibo.com> 1.25beta1-1
- update to release 1.25beta1

* Mon Sep 28 2015 Jehan-Guillaume de Rorthais <jgdr@dalibo.com> 1.24-1
- update to release 1.24

* Wed Dec 10 2014 Nicolas Thauvin <nicolas.thauvin@dalibo.com> 1.19-1
- update to release 1.19

* Fri Sep 19 2014 Nicolas Thauvin <nicolas.thauvin@dalibo.com> 1.15-1
- Initial version

