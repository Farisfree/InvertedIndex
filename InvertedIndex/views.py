from django.http import HttpResponse
from django.shortcuts import render


def search(request):
    return render(request, "search.html")


def pagination(request):
    return render(request, "pagination.html")