"""
Definition of urls for Django516.
"""



from datetime import datetime
from app import views
from django.conf.urls import url
import django.contrib.auth.views
from django.contrib import admin

import app.forms
import app.views

# Uncomment the next lines to enable the admin:
# from django.conf.urls import include
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = [
    # Examples:
    url(r'^$', app.views.home, name='home'),
    url(r'^contact$', app.views.contact, name='contact'),
    url(r'^about$', app.views.about, name='about'),
    url(r'^login/$',
        django.contrib.auth.views.login,
        {
            'template_name': 'app/login.html',
            'authentication_form': app.forms.BootstrapAuthenticationForm,
            'extra_context':
            {
                'title': 'Log in',
                'year': datetime.now().year,
            }
        },
        name='login'),
    url(r'^logout$',
        django.contrib.auth.views.logout,
        {
            'next_page': '/',
        },
        name='logout'),
    url(r'^RSI_page/',app.views.RSI_page,name='RSI_page'),
    url(r'^RSI_process/',app.views.RSI_process,name='RSI_process'),
    url(r'^KPI_process/',app.views.KPI_process,name='KPI_process'),
    url(r'^BBands/',app.views.BBands,name='BBands'),
    url(r'^BBands_py/',app.views.BBands_py,name='BBands_py'),
    url(r'^bo/',app.views.bo,name='bo'),
    url(r'^sm/',app.views.sm,name='sm'),
    url(r'^rsi/',app.views.rsi,name='rsi')
]
 #url(r'^crawl_price/',app.views.crawl_price,name='crawl_price')
    #url(r'^stockpool',app.views.stockpool,name='stockpool')
    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    # url(r'^admin/', include(admin.site.urls)),