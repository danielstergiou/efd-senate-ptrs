# Senate periodic transaction reports (eFD), fetched every two hours

Public records only: the e-filed Periodic Transaction Reports senators file with the U.S. Senate Office of Public Records,
read from https://efdsearch.senate.gov every two hours by the GitHub Actions job in this repository and saved as
`efd/latest.json` (the newest 45 days) plus a stamped copy per run. Paper filings are kept as a link to the report only.

Maintained by The Distributed (https://thedistributed.co), a news publication, for dissemination to the general public.
Every row keeps its report link; the source is the U.S. Senate Office of Public Records. 5 U.S.C. app. § 105(c) forbids
obtaining or using these reports for any unlawful purpose, for any commercial purpose other than by news and
communications media for dissemination to the general public, for credit rating, or for soliciting money: if you reuse
this file, those limits apply to you too.

Code: `pipelines/congress/efd_fetch.py` (one request a second at most; stops on the first refusal).
