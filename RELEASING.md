# Releasing

## Source code

Edit variable `$VERSION` in `check_pgactivity`

Update the version field at
the end of the inline documentation in this script.

In `check_pgactivity.spec`:
  * update the tag in the `_tag` variable (first line)
  * update the version in `Version:`
  * edit the changelog
    * date format: `LC_TIME=C date +"%a %b %d %Y"`

## Documentation

Generate updated documentation:
```
pod2text check_pgactivity > README
podselect check_pgactivity > README.pod
```

## Tagging and building tar file

Directly into the official repo:

```
TAG=REL2_4
git tag -a $TAG  <Release commit number>
git push --tags
git archive --prefix=check_pgactivity-$TAG/ -o /tmp/check_pgactivity-$TAG.tgz $TAG
```

## Release on github

  - Go to https://github.com/OPMDG/check_pgactivity/releases
  - Edit the release notes for the new tag
  - Set "check_pgactivity $VERSION" as title, eg. "check_pgactivity 2.4"
  - Here is the format of the release node itself:
    YYYY-MM-DD -  Version X.Y
    
    Changelog:
      * item 1
      * item 2
      * ...
      
  - Upload the tar file
  - Save
  - Check or update https://github.com/OPMDG/check_pgactivity/releases

## Building the RPM file

### Installation

```
yum group install "Development Tools"
yum install rpmdevtools
useradd makerpm
```

### Building the package

```
su - makerpm
rpmdev-setuptree
git clone https://github.com/OPMDG/check_pgactivity.git
spectool -R -g check_pgactivity/check_pgactivity.spec
rpmbuild -ba check_pgactivity/check_pgactivity.spec
```

The RPM is generated into `rpmbuild/RPMS/noarch`.

Don't forget to upload the package on github release page.

## Building the Debian package

Debian packaging is handled by the Debian Mainteners
(see https://salsa.debian.org/?name=check_pgactivity).
A new release will trigger the release of a new package.
