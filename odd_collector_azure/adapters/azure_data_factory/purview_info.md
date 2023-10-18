# Purview info

It was analysed if Microsoft Purview would be useful in terms of ODD.  
Currently, it looks like Purview will not add additional value to the ADF adapter.

### Reasons:

* It shows lineage only for single activities not for the whole pipelines.
* It's based on pipeline runs not on the definition itself, hence it shows lineage for each run. Because of that, the container or
  directory could change in each run.
* Lineage is shown "from file perspective", activities performed on a single file by different pipelines are shown in
  a single lineage.
* Using Purview brings additional cost for the user, and most of the metadata shown by Purview can be retrieved directly from
  ADF API, hence using Purview doesn't look profitable.