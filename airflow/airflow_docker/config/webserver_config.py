from __future__ import annotations

import os

from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride
from airflow.www.fab_security.manager import AUTH_LDAP
from flask import g, redirect, request, flash
from flask_appbuilder import expose
from flask_appbuilder._compat import as_unicode
from flask_appbuilder.security.forms import LoginForm_db
from flask_appbuilder.security.views import AuthView
from flask_appbuilder.utils.base import get_safe_redirect
from flask_login import login_user


class CustomAuthLDAPView(AuthView):
    login_template = "appbuilder/general/security/login_ldap.html"

    @expose("/login/", methods=["GET", "POST"])
    def login(self):
        if g.user is not None and g.user.is_authenticated:
            return redirect(self.appbuilder.get_url_for_index)
        form = LoginForm_db()
        if form.validate_on_submit():
            next_url = get_safe_redirect(request.args.get("next", ""))
            user = self.appbuilder.sm.auth_user_ldap(
                form.username.data, form.password.data
            )
            if not user:
                user = self.appbuilder.sm.auth_user_db(
                    form.username.data, form.password.data
                )
                if not user:
                    flash(as_unicode(self.invalid_login_message), "warning")
                    return redirect(self.appbuilder.get_url_for_login_with(next_url))
            login_user(user, remember=False)
            return redirect(next_url)
        return self.render_template(
            self.login_template, title=self.title, form=form, appbuilder=self.appbuilder
        )


class CustomSecurityManager(FabAirflowSecurityManagerOverride):
    authldapview = CustomAuthLDAPView


basedir = os.path.abspath(os.path.dirname(__file__))

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None

SECURITY_MANAGER_CLASS = CustomSecurityManager
AUTH_TYPE = AUTH_LDAP

AUTH_LDAP_SERVER = "ldap://partner.ru:389"
AUTH_LDAP_USE_TLS = False
AUTH_LDAP_FIRSTNAME_FIELD = "givenName"
AUTH_LDAP_LASTNAME_FIELD = "sn"
AUTH_LDAP_EMAIL_FIELD = "mail"
AUTH_LDAP_UID_FIELD = "sAMAccountName"
AUTH_LDAP_SEARCH = "OU=DNS Users,DC=partner,DC=ru"

AUTH_LDAP_USERNAME_FORMAT = "%s"
AUTH_LDAP_APPEND_DOMAIN = "partner.ru"

AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Public"

AUTH_ROLES_MAPPING = {
    "CN=_ IT-группа Инженеры данных,OU=IT-группа Инженеры данных,OU=Фед. коммерческая служба,OU=3. Федеральная администрация,OU=DNS Users,DC=partner,DC=ru": ["Admin"],
    "CN=_ Автоматизация комм. процессов,OU=Автоматизация комм. процессов,OU=Фед. коммерческая служба,OU=3. Федеральная администрация,OU=DNS Users,DC=partner,DC=ru": ["Op"],
    "CN=_ IT-группа Анализ данных,OU=IT-группа Анализ данных,OU=Фед. коммерческая служба,OU=3. Федеральная администрация,OU=DNS Users,DC=partner,DC=ru": ["Viewer"],
}

# ----------------------------------------------------
# Theme CONFIG
# ----------------------------------------------------
# Flask App Builder comes up with a number of predefined themes
# that you can use for Apache Airflow.
# http://flask-appbuilder.readthedocs.io/en/latest/customizing.html#changing-themes
# Please make sure to remove "navbar_color" configuration from airflow.cfg
# in order to fully utilize the theme. (or use that property in conjunction with theme)
# APP_THEME = "bootstrap-theme.css"  # default bootstrap
# APP_THEME = "amelia.css"
# APP_THEME = "cerulean.css"
# APP_THEME = "cosmo.css"
# APP_THEME = "cyborg.css"
# APP_THEME = "darkly.css"
# APP_THEME = "flatly.css"
# APP_THEME = "journal.css"
# APP_THEME = "lumen.css"
# APP_THEME = "paper.css"
# APP_THEME = "readable.css"
# APP_THEME = "sandstone.css"
# APP_THEME = "simplex.css"
# APP_THEME = "slate.css"
# APP_THEME = "solar.css"
# APP_THEME = "spacelab.css"
# APP_THEME = "superhero.css"
# APP_THEME = "united.css"
# APP_THEME = "yeti.css"
