# Report

Anonymised engineering report for the cost-of-living data platform.

| Path | Purpose |
| --- | --- |
| `source/main.tex` | LaTeX source |
| `source/references.bib` | Bibliography |
| `source/main.pdf` | Rendered report |
| `evidence/` | Charts used by the report |

Build with XeLaTeX and BibTeX:

```bash
cd report/source
xelatex main.tex
bibtex main
xelatex main.tex
xelatex main.tex
```
