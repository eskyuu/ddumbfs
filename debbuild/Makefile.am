debcheck:
	@which dh_make &> /dev/null; \
	if [ $$? -ne 0 ]; then \
	  echo "*** This make target requires an deb-based linux distribution."; \
	  (exit 1); exit 1; \
	fi

TMP_DIR = /tmp/ddumbfs.deb

deb: debcheck control
	@echo TMP_DIR=$(TMP_DIR)
	- rm -rf $(TMP_DIR)
	mkdir -p $(TMP_DIR)
	cp $(PACKAGE_TARNAME)-$(VERSION).tar.bz2 $(TMP_DIR)/
	tar --directory $(TMP_DIR)/ -xjf $(TMP_DIR)/$(APP_NAME)-$(VERSION).tar.bz2
	cd $(TMP_DIR)/$(APP_NAME)-$(VERSION)/; \
       echo | dh_make --single --copyright gpl -e myemailaddress -f \
       ../$(APP_NAME)-$(VERSION).tar.bz2
	cp install/deb/changelog install/deb/control \
        install/deb/copyright $(TMP_DIR)/$(APP_NAME)-$(VERSION)/debian/
	cd $(TMP_DIR)/$(APP_NAME)-$(VERSION)/; \
        rm debian/README.Debian debian/*.ex debian/*.EX; \
        ./configure; \
        dpkg-buildpackage -rfakeroot; \
        mv ../*.deb $(PWD)/pkg/
	rm -r $(TMP_DIR);

.PHONY: debcheck deb