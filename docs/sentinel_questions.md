# Sentinel Questions

Use this fixed question set after meaningful changes to clinical reasoning, source routing, prompt behavior, or UI flows that may affect answers/sources.

Goal:
- Catch regressions quickly
- Avoid full QA on every change
- Verify both answer quality and source grounding

## Core Set

### 1. Obstetrics acute

```text
A 31-year-old G2P1 at 33+4 weeks presents with PPROM. She is afebrile, has no uterine tenderness, fetal monitoring is reassuring, and there are no contractions. What is the most appropriate management now? Please answer in a board-prep style and include the source you are relying on.
```

Expected direction:
- Expectant inpatient management before 34 weeks
- Corticosteroids
- Latency antibiotics
- Monitoring and delivery threshold

Expected source family:
- ACOG PROM

### 2. High-risk pregnancy

```text
A 34-year-old G2P1 at 37 weeks presents with blood pressure 168/112, headache, and RUQ pain. What is the most appropriate next step in management? Please answer in a board-prep style and include the source you are relying on.
```

Expected direction:
- Severe preeclampsia with severe features
- Magnesium + acute BP treatment
- Delivery after stabilization

Expected source family:
- ACOG gestational hypertension / preeclampsia

### 3. Cervical screening

```text
A 27-year-old has HSIL cytology on routine screening. She is not pregnant. What is the most appropriate next step in management? Please answer in a board-prep style and include the source you are relying on.
```

Expected direction:
- Colposcopy is appropriate now
- Expedited treatment is risk-context dependent, not universal

Expected source family:
- ASCCP

### 4. Infectious gynecology

```text
A 26-year-old presents with lower abdominal pain, cervical motion tenderness, adnexal tenderness, and fever. What is the most likely diagnosis and the most appropriate initial management? Please answer in a board-prep style and include the source you are relying on.
```

Expected direction:
- PID
- Empiric treatment should not be delayed
- Evaluate ectopic / TOA as needed, not as a reason to postpone treatment

Expected source family:
- CDC PID

### 5. Benign gynecology

```text
A 47-year-old presents with abnormal uterine bleeding. What is the most appropriate next step in evaluation? Please answer in a board-prep style and include the source you are relying on.
```

Expected direction:
- Endometrial sampling is central in age 45+
- TVUS may be complementary

Expected source family:
- ACOG AUB

### 6. Fertility workup

```text
A 32-year-old has been trying to conceive for 13 months with regular unprotected intercourse. She has regular cycles and no prior pregnancies. What is the most appropriate next step in evaluation? Please answer in a board-prep style and include the source you are relying on.
```

Expected direction:
- Infertility evaluation now
- Couple evaluation
- Semen analysis + female workup in parallel

Expected source family:
- ASRM fertility evaluation

### 7. Fertility escalation / IVF pathway

```text
A 36-year-old has been trying to conceive for 2 years. Her HSG is normal, semen analysis is normal, and ovarian reserve testing is reassuring. What is the most appropriate next step in management? Please answer in a board-prep style and include the source you are relying on.
```

Expected direction:
- Unexplained infertility
- Escalate to ovarian stimulation with IUI before IVF in many board-style cases

Expected source family:
- ASRM unexplained infertility / fertility evaluation

### 8. Early pregnancy

```text
A 29-year-old presents with a positive pregnancy test, mild spotting, and no intrauterine pregnancy on transvaginal ultrasound. She is hemodynamically stable. What is the most appropriate next step in management? Please answer in a board-prep style and include the source you are relying on.
```

Expected direction:
- PUL
- Quantitative hCG now and again in 48 hours
- Safety netting and repeat ultrasound

Expected source family:
- NICE ectopic pregnancy / miscarriage

### 9. Menopause / PMB

```text
A 58-year-old presents with new postmenopausal bleeding. What is the most appropriate next step in evaluation? Please answer in a board-prep style and include the source you are relying on.
```

Expected direction:
- Malignancy exclusion
- Endometrial biopsy central
- TVUS complementary, not reassurance alone

Expected source family:
- ACOG postmenopausal bleeding

### 10. Gynecologic oncology

```text
A 52-year-old presents with abdominal bloating, early satiety, and a new adnexal mass on pelvic ultrasound. What is the most appropriate next step in management? Please answer in a board-prep style and include the source you are relying on.
```

Expected direction:
- High concern for malignancy
- CA-125 / risk stratification
- Gynecologic oncology referral

Expected source family:
- ACOG adnexal masses

### 11. Endometriosis

```text
A 30-year-old presents with chronic cyclic pelvic pain, dysmenorrhea, and dyspareunia. What is the most likely diagnosis and the most appropriate initial management? Please answer in a board-prep style and include the source you are relying on.
```

Expected direction:
- Endometriosis
- Empiric NSAIDs + hormonal suppression before routine laparoscopy

Expected source family:
- ACOG endometriosis

### 12. Obstetric ultrasound

```text
A 29-year-old at 20 weeks has an anatomy scan showing a low-lying placenta. She is asymptomatic. What is the most appropriate next step in management? Please answer in a board-prep style and include the source you are relying on.
```

Expected direction:
- Follow-up imaging, not immediate intervention
- Repeat transvaginal ultrasound around the third trimester

Expected source family:
- AIUM / obstetric ultrasound source

## Pass Criteria

- The answer is clinically coherent and board-style
- The answer does not contradict the source family
- A clearly matching authoritative source is shown
- No empty reply
- No broken formatting
- No conversational carry-over like "same as above" or "this is a repeat"

## Quick Smoke Subset

If you only want a fast check after a risky code change, use:
- PPROM
- severe preeclampsia
- HSIL
- infertility evaluation
- PUL
- adnexal mass

## Overlap Sentinel Set

Use this set after changes to overlap coverage, source routing, or prompt boundaries.

### 1. Lower UTI outside pregnancy

```text
What is the first-line treatment for UTI in a nonpregnant woman? Please answer directly and include the source you are relying on.
```

Expected direction:
- Answer directly instead of calling it out of scope
- Uncomplicated cystitis framing
- First-line oral therapy with local resistance guidance

Expected source family:
- NICE lower UTI or another authoritative lower-UTI source

### 2. Pyelonephritis in pregnancy

```text
A 24-week pregnant patient has fever, flank pain, and CVA tenderness. What is the most appropriate management now? Please answer directly and include the source you are relying on.
```

Expected direction:
- Recognize pyelonephritis
- Inpatient treatment with parenteral antibiotics
- Maternal-fetal monitoring

Expected source family:
- ACOG UTI in pregnancy

### 3. Postpartum severe hypertension

```text
Five days postpartum, a patient has blood pressure 170/112, severe headache, and visual symptoms. What is the most appropriate next step? Please answer directly and include the source you are relying on.
```

Expected direction:
- Postpartum preeclampsia with severe features
- Acute blood pressure treatment
- Magnesium prophylaxis

Expected source family:
- ACOG preeclampsia

### 4. DVT and contraception

```text
A 3-week postpartum breastfeeding patient had a postpartum DVT and asks for combined oral contraceptives. What is the best next step? Please answer directly and include the source you are relying on.
```

Expected direction:
- Avoid estrogen-containing contraception
- Offer a non-estrogen method

Expected source family:
- CDC U.S. MEC

### 5. Platelets and epidural

```text
A laboring patient with severe preeclampsia has platelets of 68,000 and requests epidural analgesia. What is the best next step? Please answer directly and include the source you are relying on.
```

Expected direction:
- Recognize thrombocytopenia changes neuraxial decision-making
- Do not frame it as routine epidural placement
- Do not halt the whole obstetric plan

Expected source family:
- SOAP thrombocytopenia / neuraxial consensus

### 6. Lactation medication safety

```text
Is nitrofurantoin generally compatible with breastfeeding? Please answer directly and include the source you are relying on.
```

Expected direction:
- Answer directly
- Use a drug-safety/lactation source
- Mention brief safety framing rather than refusing

Expected source family:
- LactMed / drug-safety reference

### 7. Water intoxication in labor

```text
During a prolonged induction with oxytocin and large volumes of hypotonic fluid, a laboring patient becomes confused and nauseated. What is the most likely problem? Please answer directly and include the source you are relying on.
```

Expected direction:
- Recognize water intoxication / hyponatremia
- Do not mislabel automatically as an obstetric catastrophe

Expected source family:
- Hyponatremia in labor guidance

### 8. Guard check

Pass criteria for overlap:
- The answer does not say "outside my scope" when authoritative overlap sources are available
- A directly relevant source is shown
- The answer is concise and useful, not evasive

### 9. Lactation medication safety

```text
Is nitrofurantoin generally compatible with breastfeeding? Please answer directly and include the source you are relying on.
```

Expected direction:
- Answer directly
- Use a lactation/drug-safety source
- Avoid evasive or out-of-scope framing

Expected source family:
- LactMed

### 10. VTE and estrogen contraception

```text
A patient with a recent postpartum DVT asks to start combined hormonal contraception. What is the best next step? Please answer directly and include the source you are relying on.
```

Expected direction:
- Avoid estrogen-containing contraception
- Offer a non-estrogen option
- Avoid saying this is outside scope

Expected source family:
- CDC U.S. MEC

### 11. Atosiban and magnesium neuroprotection

```text
Is it appropriate to give atosiban without magnesium sulfate neuroprotection in threatened preterm labor? Please answer directly and include the source you are relying on.
```

Expected direction:
- Separate tocolysis from fetal neuroprotection
- Magnesium decision depends on gestational age and imminence of birth
- No out-of-scope language

Expected source family:
- NICE preterm labour and birth or ACOG magnesium sulfate neuroprotection / preterm labor
