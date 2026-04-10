# Open Tasks

## Active

1. Stabilize textbook shorthand handling with a long-term resolver
   - Goal: extend the new resolver into a fuller reusable clinical shorthand and intent-resolution layer.
   - Current status: baseline resolver and normalization layer added for textbook prompts.
   - Desired outcome:
     - normalize shorthand such as `pet`, `pec`, `pph`, `gdm`, `pprom`
     - support telegraphic book prompts such as `pph gabbe` or `amenorrhea speroff`
     - reduce false positives from short tokens

2. Add regression coverage for textbook routing edge cases
   - Goal: extend the new smoke script into broader repeatable regression coverage for textbook intent detection and source selection.
   - Current status: initial smoke script exists for core textbook routing prompts.
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
