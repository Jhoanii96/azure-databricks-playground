# Databricks Cluster Dependencies

Centralized pattern for managing Python packages and ODBC drivers in Databricks clusters.

## Overview

Python libraries and ODBC drivers are uploaded to a Unity Catalog Volume and installed automatically through cluster init scripts. This provides repeatable dependency management and a governed, centralized setup across cluster restarts and environments.