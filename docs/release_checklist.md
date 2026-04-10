# Release Checklist

## Goal
Use this checklist any time we promote textbook or UI changes from `develop` to live.

## Pre-Release On Develop

1. Finish implementation on `develop`.
2. Run targeted QA on `dev`.
3. Run textbook routing smoke checks:

```bash
./venv/bin/python scripts/run_textbook_routing_smoke.py
```
4. Verify textbook behavior on `dev` for supported books:
   - `Gabbe`
   - `Speroff`
5. Verify smoke flows on `dev`:
   - textbook question
   - regular chat question
   - `+ -> Open cards`
   - source rendering

## Promote Code

1. Push `develop`.
2. Merge or promote `develop` into `main`.
3. Push `main`.
4. Wait for production deploy to finish.

## Production Textbook Cache

Important: code deploy does not populate `production` textbook cache automatically.

If textbook mapping, page cache, or supported textbook coverage changed, rebuild production cache.

### Gabbe

1. Build page cache:

```bash
APP_ENV=production python scripts/rebuild_gabbe_page_cache.py --target-page 1432 --batch-size 25
```

2. Build topic mapping tier A:

```bash
APP_ENV=production python scripts/rebuild_gabbe_topic_mapping.py --tier A --batch-size 5
```

3. Build topic mapping tier B:

```bash
APP_ENV=production python scripts/rebuild_gabbe_topic_mapping.py --tier B --batch-size 5
```

### Speroff

1. Build page cache:

```bash
APP_ENV=production python scripts/rebuild_speroff_page_cache.py --target-page 3505 --batch-size 25
```

2. Build topic mapping tier A:

```bash
APP_ENV=production python scripts/rebuild_speroff_topic_mapping.py --tier A --batch-size 5
```

3. Build topic mapping tier B:

```bash
APP_ENV=production python scripts/rebuild_speroff_topic_mapping.py --tier B --batch-size 5
```

## Production Data Verification

In Mongo production DB (`clinical_assistant`), verify `textbook_cache` contains expected docs:

- `gabbe_page_text`
- `gabbe_topic_mapping`
- `speroff_10_page_text`
- `speroff_10_topic_mapping`

## Live Smoke Test

Run at least one question per supported textbook:

### Gabbe

```text
According to Gabbe, how is PPROM managed?
```

### Speroff

```text
According to Speroff, how is PCOS managed?
```

Also verify:

- textbook answers cite correct textbook excerpts
- sources do not show placeholder update dates
- `+ -> Open cards` works
- study cards do not reappear during thinking

## Done Definition

Release is only complete when:

1. `main` is deployed
2. production textbook cache is populated
3. live smoke tests pass
