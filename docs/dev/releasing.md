# Releasing

## Source code

Edit variable `$VERSION` in `check_pgactivity`, and update the version field at
the end of the inline documentation in this script.

In `check_pgactivity.spec` :
  * update the tag in the `_tag` variable (first line)
  * update the version in `Version:`
  * edit the changelog
    * date format: `LC_TIME=C date +"%a %b %d %Y"`

~~In `debian/`, edit the `changelog` file~~

## Documentation

Generate updated documentation :
```
pod2text check_pgactivity > README
podselect check_pgactivity > README.pod
```

## Tagging and building tar file

```
TAG=REL2.2
git -a $TAG
git push --tags
git archive --prefix=PAF-$TAG/ -o /tmp/PAF-$TAG.tgz $TAG
```

## Release on github

  - go to (https://github.com/OPMDG/check_pgactivity/tags)
  - edit the release notes for the new tag
  - set "check_pgactivity $VERSION" as title, eg. "check_pgactivity 2.2"
  - here is the format of the release node itself:
    YYYY-MM-DD -  Version X.Y
    
    Changelog:
      * item 1
      * item 2
      * ...
      
      See http://opmdg.github.io/checkpg_activity/documentation.html
  - upload the tar file
  - save

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
spectool -R -g check_pgactivity/resource-agents-paf.spec
rpmbuild -ba check_pgactivity/resource-agents-paf.spec
```

Don't forget to upload the package on github release page.

