## Introduction

This module contains xgboost, gdbt, prophet, agglomerative, This module is compatible with openGauss, postgresql, and greenplum.



#### Additional packages

 1) if you use facebook prophet

```
pip install pystan
pip install holidays==0.9.8
pip install fbprophet==0.4
```

 2) if you use xgboost

```
pip install pandas
pip install scikit-learn
pip install xgboost
```


### How to use

```
cd madlib_modules
cp -r * YOUR_MADLIB_SOURCE_CODE/src/ports/postgres/modules
```

THEN, add following to `src/config/Modules.yml` to register those modules.

```
- name: agglomerative_clustering
    depends: ['utilities']
- name: xgboost_gs
    depends: ['utilities']
- name: facebook_prophet
    depends: ['utilities']
- name: gbdt
    depends: ['utilities', 'recursive_partitioning']
```

Next, compile and your MADlib as usual.

Finally, run madpack to install.
