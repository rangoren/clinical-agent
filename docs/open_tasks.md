# Open Tasks

## Active

1. Stabilize textbook shorthand handling with a long-term resolver
   - Goal: replace growing manual phrase lists with a reusable clinical shorthand and intent-resolution layer.
   - Why: short-term aliases now cover cases like `by Gabbe`, `Gabbe on`, `pet`, and `pec`, but this approach will keep expanding unless we centralize it.
   - Desired outcome:
     - normalize shorthand such as `pet`, `pec`, `pph`, `gdm`, `pprom`
     - support telegraphic book prompts such as `pph gabbe` or `amenorrhea speroff`
     - reduce false positives from short tokens

2. Add regression coverage for textbook routing edge cases
   - Goal: create repeatable tests or scripted smoke checks for textbook intent detection and source selection.
   - Priority cases:
     - `When to induce labor in early severe pet by Gabbe`
     - `Gabbe on severe pec`
     - `pph gabbe`
     - `pcos speroff`
     - Hebrew and mixed-language prompts

3. Improve production release automation for textbook cache
   - Goal: reduce manual release work for production textbook cache rebuilds.
   - Desired outcome:
     - one documented bootstrap command or scripted release step
     - fewer chances to deploy code without production cache

## Deferred

1. Resume alternative extraction path for `Berek`
   - Current status: `PyMuPDF` did not recover usable text from the current PDF.
   - Next best path:
     - obtain a better PDF with a valid text layer
     - otherwise evaluate an OCR-based preprocessing pipeline
