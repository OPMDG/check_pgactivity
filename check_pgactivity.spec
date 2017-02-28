%global _tag REL2_2_RC1

Name: nagios-plugins-pgactivity
Version: 2.2~rc1
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
%doc README LICENSE

%changelog
* Tue Feb 28 2017 Jehan-Guillaume de Rorthais <jgdr@dalibo.com> 2.2~rc1-1
- new release candidate 2.2~rc1

* Mon Aug 29 2016 Jehan-Guillaume de Rorthais <jgdr@dalibo.com> 2.0-1
- new major release 2.0

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

