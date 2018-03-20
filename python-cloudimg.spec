%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}

Name:           python-cloudimg
Version:        0.2.4
Release:        1%{?dist}
Summary:        A library for uploading and publishing disk images on various clouds

Group:          Development/Languages
License:        GPLv3
URL:            https://gitlab.cee.redhat.com/rad/cloud-image/
Source0:        %{name}-%{version}.tar.gz

BuildArch:      noarch

Requires:	python-requests
Requires:	python-boto3

BuildRequires:  python-setuptools
BuildRequires:  python2-devel

%description
cloudimg is a Python library capable of uploading disk images to various
cloud providers and publishing/distributing them in different ways.

%prep
%setup -qn %{name}-%{version}

%build
%{__python} setup.py build

%install
%{__python} setup.py install -O1 --skip-build --root=$RPM_BUILD_ROOT
 
%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%doc LICENSE README.md
%{python_sitelib}/*

%changelog
* Tue Mar 20 2018 Alex Misstear <amisstea@redhat.com> 0.2.4-1
- Wait for propagation delay after AWS bucket creation (amisstea@redhat.com)

* Fri Mar 16 2018 Alex Misstear <amisstea@redhat.com> 0.2.3-1
- Improved log message for file uploads (amisstea@redhat.com)

* Fri Mar 16 2018 Alex Misstear <amisstea@redhat.com> 0.2.2-1
- Wait for bucket to exist after creation (amisstea@redhat.com)
- Improve the upload progress callback (amisstea@redhat.com)

* Tue Mar 13 2018 Alex Misstear <amisstea@redhat.com> 0.2.1-1
- Fix source package name to work with tito (amisstea@redhat.com)

* Tue Mar 13 2018 Alex Misstear <amisstea@redhat.com> 0.2.0-1
- boto3 now used for AWS

* Tue Jan 2 2018 Alex Misstear <amisstea@redhat.com> - 0.1.4-2
- New version of python-libcloud dependency

* Wed May 24 2017 Alex Misstear <amisstea@redhat.com> - 0.1.4-1
- AWS storage driver determined from region

* Wed Apr 5 2017 Alex Misstear <amisstea@redhat.com> - 0.1.3-1
- Fixed SR-IOV net support default value

* Tue Apr 4 2017 Alex Misstear <amisstea@redhat.com> - 0.1.2-1
- ENA and SR-IOV net support enabled by default on AWS images

* Tue Mar 14 2017 Alex Misstear <amisstea@redhat.com> - 0.1.1-1
- Fix sharing images with an empty account list

* Thu Mar 9 2017 Alex Misstear <amisstea@redhat.com> - 0.1.0-3
- Support for all regions in AWS

* Thu Mar 2 2017 Alex Misstear <amisstea@redhat.com> - 0.1.0-2
- Full support for publishing images to AWS

* Wed Feb 15 2017 Alex Misstear <amisstea@redhat.com> - 0.1.0-1
- Uploading of AWS images to storage
