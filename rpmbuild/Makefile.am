rpmspec = $(PACKAGE_TARNAME).spec

rpmmacros =\
 --define='_rpmdir $(PWD)'\
 --define='_srcrpmdir $(PWD)'\
 --define='_sourcedir $(PWD)'\
 --define='_specdir $(PWD)'\
 --define='_builddir $(PWD)'

rpmopts = --nodeps --buildroot='$(PWD)/_rpm'

rpmcheck:
	@which rpmbuild &> /dev/null; \
	if [ $$? -ne 0 ]; then \
	  echo "*** This make target requires an rpm-based linux distribution."; \
	  (exit 1); exit 1; \
	fi

srcrpm: rpmcheck $(rpmspec)
	rpmbuild -bs $(rpmmacros) $(rpmopts) $(rpmspec)

rpms: rpmcheck $(rpmspec)
	@echo rpmbuild -ba $(rpmmacros) $(rpmopts) $(rpmspec)
	rpmbuild -ba $(rpmmacros) $(rpmopts) $(rpmspec)

.PHONY: rpmcheck srcrpm rpms