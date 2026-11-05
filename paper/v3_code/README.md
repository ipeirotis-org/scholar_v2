# v3 code (Sen Tian, 2021), kept for the record

The R scripts behind arXiv:2103.16025 v3, copied unchanged from
[sentian/SciImpactRanking](https://github.com/sentian/SciImpactRanking) at commit
[`2aaf31d`](https://github.com/sentian/SciImpactRanking/tree/2aaf31d0940ba10e30869433f64e788b642f37a0)
("revision 2", 2021-12-16), the last commit in that repository. None of the current analyses use them: `../sql/`
recomputes everything on S2.

## Files

- **`utils.R`** loads the R packages and defines the functions:
  - `calc.pubrp`: paper rank percentiles by age, for three benchmarks (all, biology, tenured);
  - `calc.autrp` and `calc.autrp.future`: the scholar indicators S_c, S_h and S_P5, and the future-work version;
  - `perc.rank`: the Hazen percentile, (rank − 0.5) / n, with uncited papers set to 0;
  - the error measures (RMSE, MAE, RMedSE).
- **`calcRP.R`** reads `data/raw/` and writes the percentile tables to `data/rp/*.rds`.
- **`plots.R`** draws the figures and tables of v3 and its supplement from `data/rp/` and `results/errors.rds`.
- **`revision/calcRP-median.R`** is an older copy of `calcRP.R` with the functions inline.

The scripts expect the upstream layout (`setwd('SciImpactRanking/')`), so run them from a checkout of that
repository.

## Not copied

- **Data.** `data/raw/` holds the 2017 Google Scholar crawl: `citations.csv` (250 MB), `professors.csv` (4.1 MB) and
  `authors.csv` (0.7 MB). `data/rp/` holds the computed percentiles (117 MB) and `results/errors.rds` the model
  errors (0.1 MB). All are Git LFS objects; a plain clone gets only the pointer files. The upstream README also links
  Dropbox copies of the [raw data](https://www.dropbox.com/sh/6nb9cujt526vvap/AAD9lHm87p4S0k2mh6Iv5CzDa?dl=0) and the
  [percentiles](https://www.dropbox.com/sh/f91ku76yz7ppub6/AAAB4YtXkOkQlbYit0-Wra2ya?dl=0); these links were not
  checked.
- **LaTeX.** The upstream `paper/` folder matches `../manuscript/`, apart from one supplementary figure and two
  bibliography entries added here.

## Missing upstream

The code that fits the prediction models of v3 Fig. 8 (the 9:1 split and the 75 fits) is not in the repository.
`plots.R` only reads their precomputed errors from `results/errors.rds`.
