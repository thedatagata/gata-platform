{{ config(materialized='table') }}

{#
    Shell: Wayne Enterprises Analytics Session Report
    Factory: build_analytics_fact
    Sources: google_analytics
    Logic: conversion_events includes generate_lead
#}

{{ build_analytics_fact('wayne_enterprises') }}
