from django.contrib import admin
from django.urls import path, include
from . import views
urlpatterns = [
    path('', views.index, name='index'),
    path('dashboard/',views.dashboard ,name='dashboard'),
    path('logout/',views.logout_view ,name='logout'),
    path('mail_data/',views.mail_data ,name='mail_data'),
    path('mail_billed_data/',views.mail_billed_data ,name='mail_billed_data'),
    path('add_new_operator/',views.add_new_operator ,name='add_new_operator'),
    path('add_new_operator_ajax/',views.add_new_operator_ajax ,name='add_new_operator_ajax'),
    path('get_non_email_op_list/',views.get_non_email_op_list ,name='get_non_email_op_list'),
]
# Function for merge sort
