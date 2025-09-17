

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ac151d2e-083e-4c6d-a35d-5eca62125ad0"),
    pre_post_dx_final=Input(rid="ri.foundry.main.dataset.8b25c6bd-bcbd-41e4-a6d9-c9637cc8bb0b")
)
SELECT *, 

CASE
    WHEN condition_concept_name = 'Disorder of nervous system due to type 2 diabetes mellitus' THEN 'Diabetes complications'
    WHEN condition_concept_name = 'Atherosclerosis of coronary artery without angina pectoris' THEN 'Atherosclerosis of coronary artery'
    WHEN condition_concept_name = 'Diverticulosis of large intestine without diverticulitis' THEN 'Diverticulosis of large intestine'
    WHEN condition_concept_name = 'Benign prostatic hypertrophy with outflow obstruction' THEN 'Benign prostatic hypertrophy'
    WHEN condition_concept_name = 'Abnormal findings on diagnostic imaging of lung' THEN 'Abnormal findings on diagnostic imaging of lung'
    WHEN condition_concept_name = 'Polyneuropathy due to type 2 diabetes mellitus' THEN 'Diabetes complications'
    WHEN condition_concept_name = 'Dehydration' THEN 'Dehydration'
    WHEN condition_concept_name = 'Gastroesophageal reflux disease without esophagitis' THEN 'Gastroesophageal reflux'
    WHEN condition_concept_name = 'Constipation' THEN 'Constipation'
    WHEN condition_concept_name = 'Atelectasis' THEN 'Atelectasis'
    WHEN condition_concept_name = 'Hyperglycemia due to type 2 diabetes mellitus' THEN 'Diabetes complications'
    WHEN condition_concept_name = 'Essential hypertension' THEN 'Hypertension'
    WHEN condition_concept_name = 'Chronic kidney disease due to hypertension' THEN 'Chronic kidney disease'
    WHEN condition_concept_name = 'Type 2 diabetes mellitus' THEN 'Type 2 diabetes'
    WHEN condition_concept_name = 'Type 2 diabetes mellitus without complication' THEN 'Type 2 diabetes'
    WHEN condition_concept_name = 'Seasonal allergic rhinitis' THEN 'Allergies'
    WHEN condition_concept_name = 'Osteoarthritis of knee' THEN 'Osteoarthritis '
    WHEN condition_concept_name = 'Gastro-esophageal reflux disease with esophagitis' THEN 'Gastroesophageal reflux'
    WHEN condition_concept_name = 'Left lower quadrant pain' THEN 'Lower left quadrant pain'
    WHEN condition_concept_name = 'Disorder of upper gastrointestinal tract' THEN 'Upper GI disorder'
    WHEN condition_concept_name = 'Postoperative state' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Second trimester pregnancy' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Anemia in chronic kidney disease' THEN 'Chronic kidney disease'
    WHEN condition_concept_name = 'Non-toxic uninodular goiter' THEN 'Goiter'
    WHEN condition_concept_name = 'Uncomplicated moderate persistent asthma' THEN 'Asthma'
    WHEN condition_concept_name = 'Finding related to pregnancy' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Chronic kidney disease stage 3' THEN 'Chronic kidney disease'
    WHEN condition_concept_name = 'Single live birth' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Paresthesia' THEN 'Paresthesia'
    WHEN condition_concept_name = 'Tear film insufficiency' THEN 'Tear film insufficiency'
    WHEN condition_concept_name = 'Acquired absence of organ' THEN 'Acquired absence of organ'
    WHEN condition_concept_name = 'Degeneration of lumbar intervertebral disc' THEN 'Degeneration of lumbar intevertebral disc'
    WHEN condition_concept_name = 'Neck pain' THEN 'Neck pain'
    WHEN condition_concept_name = 'Impacted cerumen' THEN 'Impacted cerumen'
    WHEN condition_concept_name = 'Abnormal findings on diagnostic imaging of breast' THEN 'Abnormal findings on diagnostic imaging of breast'
    WHEN condition_concept_name = 'Genitourinary tract hemorrhage' THEN 'Genitourinary tract hemmorrhage'
    WHEN condition_concept_name = 'Moderate recurrent major depression' THEN 'Major depression'
    WHEN condition_concept_name = 'Urinary tract infectious disease' THEN 'Urinary tract infectious disease'
    WHEN condition_concept_name = 'Melanocytic nevus' THEN 'Melanocytic nevus'
    WHEN condition_concept_name = 'Acquired hypothyroidism' THEN 'Hypothyroidism'
    WHEN condition_concept_name = 'Noninflammatory disorder of the vagina' THEN 'Noninflammatory disorder of the vagina'
    WHEN condition_concept_name = 'Chronic kidney disease due to type 2 diabetes mellitus' THEN 'Diabetes complications'
    WHEN condition_concept_name = 'Snoring' THEN 'Snoring'
    WHEN condition_concept_name = 'Complication due to diabetes mellitus' THEN 'Diabetes complications'
    WHEN condition_concept_name = 'General finding of observation of patient' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Shoulder joint pain' THEN 'Joint pain'
    WHEN condition_concept_name = 'Paroxysmal atrial fibrillation' THEN 'Atrial fibrillation'
    WHEN condition_concept_name = 'Localized enlarged lymph nodes' THEN 'Enlarged lymph nodes'
    WHEN condition_concept_name = 'Low back pain' THEN 'Back pain'
    WHEN condition_concept_name = 'Hypoxemia' THEN 'Hypoxemia'
    WHEN condition_concept_name = 'Complication occurring during pregnancy' THEN 'Pregnancy complication'
    WHEN condition_concept_name = 'Bacterial infectious disease' THEN 'Bacterial infectious disease'
    WHEN condition_concept_name = 'Nausea and vomiting' THEN 'Nausea and vomiting'
    WHEN condition_concept_name = 'Dependence on enabling machine or device' THEN 'Dependence on enabling machine or device'
    WHEN condition_concept_name = 'Anesthesia of skin' THEN 'Anesthesia of skin'
    WHEN condition_concept_name = 'Acute pharyngitis' THEN 'Acute pharyngitis'
    WHEN condition_concept_name = 'Coronary artery graft present' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Pain in thoracic spine' THEN 'Back pain'
    WHEN condition_concept_name = 'Pure hypercholesterolemia' THEN 'Pure hypercholesterolemia'
    WHEN condition_concept_name = 'Chronic congestive heart failure' THEN 'Heart failure'
    WHEN condition_concept_name = 'Arthralgia of the ankle and/or foot' THEN 'Joint pain'
    WHEN condition_concept_name = 'Iron deficiency anemia due to blood loss' THEN 'Iron deficiency anemia due to blood loss'
    WHEN condition_concept_name = 'Disorder of bone' THEN 'Disorder of bone'
    WHEN condition_concept_name = 'Persistent pain following procedure' THEN 'Persistent pain following procedure'
    WHEN condition_concept_name = 'Vitamin D deficiency' THEN 'Vitamin D deficiency'
    WHEN condition_concept_name = 'Old myocardial infarction' THEN 'Old myocardial infarction'
    WHEN condition_concept_name = 'Pain in left foot' THEN 'Limb or extremity pain'
    WHEN condition_concept_name = 'Benign prostatic hyperplasia' THEN 'Benign prostatic hyperplasia'
    WHEN condition_concept_name = 'Posttraumatic stress disorder' THEN 'PTSD'
    WHEN condition_concept_name = 'First trimester pregnancy' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Acute upper respiratory infection' THEN 'Acute upper respiratory infection'
    WHEN condition_concept_name = 'Preoperative state' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'No abnormality detected - examination result' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Mixed hyperlipidemia' THEN 'Mixed hyperlipidemia'
    WHEN condition_concept_name = 'Finding of frequency of urination' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Onychomycosis due to dermatophyte' THEN 'Onychomycosis'
    WHEN condition_concept_name = 'Osteoarthritis of hip' THEN 'Osteoarthritis '
    WHEN condition_concept_name = 'Chronic diastolic heart failure' THEN 'Heart failure'
    WHEN condition_concept_name = 'Acute renal failure syndrome' THEN 'Renal failure'
    WHEN condition_concept_name = 'Mild intermittent asthma' THEN 'Asthma'
    WHEN condition_concept_name = 'Loss of consciousness' THEN 'Loss of consciousness'
    WHEN condition_concept_name = 'Inflammatory dermatosis' THEN 'Inflammatory dermatosis'
    WHEN condition_concept_name = 'End-stage renal disease' THEN 'End-stage renal disease'
    WHEN condition_concept_name = 'History of clinical finding in subject' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Alcohol abuse' THEN 'Alcohol abuse'
    WHEN condition_concept_name = 'Fatigue' THEN 'Fatigue'
    WHEN condition_concept_name = 'Hypo-osmolality and or hyponatremia' THEN 'Hypo-osmolality and or hyponatremia'
    WHEN condition_concept_name = 'Right upper quadrant pain' THEN 'Upper right quadrant pain'
    WHEN condition_concept_name = 'Pain in pelvis' THEN 'Pelvic pain'
    WHEN condition_concept_name = 'Obstructive sleep apnea syndrome' THEN 'Sleep apnea'
    WHEN condition_concept_name = 'Chronic obstructive lung disease' THEN 'Chronic obstructive lung disease'
    WHEN condition_concept_name = 'Spinal stenosis of lumbar region' THEN 'Spinal stenosis of lumbar region'
    WHEN condition_concept_name = 'Chronic pain' THEN 'Chronic pain'
    WHEN condition_concept_name = 'Neoplasm of uncertain behavior of skin' THEN 'Neoplasm of uncertain behavior of skin'
    WHEN condition_concept_name = 'Asthenia' THEN 'Asthenia'
    WHEN condition_concept_name = 'Peripheral vascular disease' THEN 'Peripheral vascular disease'
    WHEN condition_concept_name = 'Long-term current use of drug therapy' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Peripheral circulatory disorder due to type 2 diabetes mellitus' THEN 'Diabetes complications'
    WHEN condition_concept_name = 'Disorder of intestine' THEN 'Disorder of intestine'
    WHEN condition_concept_name = 'Seizure' THEN 'Seizure'
    WHEN condition_concept_name = 'Hyperkalemia' THEN 'Hyperkalemia'
    WHEN condition_concept_name = 'Kidney stone' THEN 'Kidney stone'
    WHEN condition_concept_name = 'Visual disturbance' THEN 'Visual disturbance'
    WHEN condition_concept_name = 'Musculoskeletal finding' THEN 'Musculoskeletal finding'
    WHEN condition_concept_name = 'Polyp of colon' THEN 'Polyp of colon'
    WHEN condition_concept_name = 'Pain in left lower limb' THEN 'Limb or extremity pain'
    WHEN condition_concept_name = 'Allergic rhinitis' THEN 'Allergies'
    WHEN condition_concept_name = 'Nausea' THEN 'Nausea and vomiting'
    WHEN condition_concept_name = 'Benign essential hypertension' THEN 'Hypertension'
    WHEN condition_concept_name = 'Major depression, single episode' THEN 'Major depression'
    WHEN condition_concept_name = 'Fibromyalgia' THEN 'Fibromyalgia'
    WHEN condition_concept_name = 'Acute posthemorrhagic anemia' THEN 'Acute posthemorrhagic anemia'
    WHEN condition_concept_name = 'Radiology result abnormal' THEN 'Radiology result abnormal'
    WHEN condition_concept_name = 'Cardiac arrhythmia' THEN 'Cardiac arrhythmia'
    WHEN condition_concept_name = 'High risk pregnancy' THEN 'High risk pregnancy'
    WHEN condition_concept_name = 'Polyneuropathy' THEN 'Polyneuropathy'
    WHEN condition_concept_name = 'Allergic disposition' THEN 'Allergies'
    WHEN condition_concept_name = 'Menopause present' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Thrombocytopenic disorder' THEN 'Thrombocytopenic disorder'
    WHEN condition_concept_name = 'Heart failure' THEN 'Heart failure'
    WHEN condition_concept_name = 'Disorder of pregnancy' THEN 'Pregnancy complication'
    WHEN condition_concept_name = 'Epigastric pain' THEN 'Abdominal pain'
    WHEN condition_concept_name = 'Lumbar spondylosis' THEN 'Lumbar spondylosis'
    WHEN condition_concept_name = 'Congestive heart failure' THEN 'Heart failure'
    WHEN condition_concept_name = 'Chronic sinusitis' THEN 'Chronic sinusitis'
    WHEN condition_concept_name = 'Uncomplicated asthma' THEN 'Asthma'
    WHEN condition_concept_name = 'Pain' THEN 'Pain'
    WHEN condition_concept_name = 'Localized edema' THEN 'Edema'
    WHEN condition_concept_name = 'Hypertensive heart failure' THEN 'Heart failure'
    WHEN condition_concept_name = 'Knee pain' THEN 'Joint pain'
    WHEN condition_concept_name = 'Disorder of kidney and/or ureter' THEN 'Disorder of kidney and/or ureter'
    WHEN condition_concept_name = 'Vitamin B deficiency' THEN 'Vitamin B deficiency'
    WHEN condition_concept_name = 'Epilepsy' THEN 'Epilepsy'
    WHEN condition_concept_name = 'Family history of clinical finding' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Generalized abdominal pain' THEN 'Abdominal pain'
    WHEN condition_concept_name = 'Electrocardiogram abnormal' THEN 'Electrocardiogram abnormal'
    WHEN condition_concept_name = 'Bradycardia' THEN 'Bradycardia'
    WHEN condition_concept_name = 'Generalized anxiety disorder' THEN 'Anxiety'
    WHEN condition_concept_name = 'Irregular periods' THEN 'Irregular periods'
    WHEN condition_concept_name = 'Blood glucose abnormal' THEN 'Blood glucose abnormal'
    WHEN condition_concept_name = 'Pleural effusion' THEN 'Pleural effusion'
    WHEN condition_concept_name = 'Disorder of body system' THEN 'Disorder of body system'
    WHEN condition_concept_name = 'Right lower quadrant pain' THEN 'Lower right quadrant pain'
    WHEN condition_concept_name = 'Third trimester pregnancy' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Impaired fasting glycemia' THEN 'Impaired fasting glycemia'
    WHEN condition_concept_name = 'Pain in left knee' THEN 'Joint pain'
    WHEN condition_concept_name = 'Migraine without aura' THEN 'Migraine'
    WHEN condition_concept_name = 'Blood chemistry abnormal' THEN 'Blood chemistry abnormal'
    WHEN condition_concept_name = 'Disorder of nasal cavity' THEN 'Disorder of nasal cavity'
    WHEN condition_concept_name = 'Pain in right lower limb' THEN 'Limb or extremity pain'
    WHEN condition_concept_name = 'Nicotine dependence' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Acidosis' THEN 'Acidosis'
    WHEN condition_concept_name = 'Lower abdominal pain' THEN 'Abdominal pain'
    WHEN condition_concept_name = 'Hypokalemia' THEN 'Hypokalemia'
    WHEN condition_concept_name = 'Exposure to viral disease' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Urogenital finding' THEN 'Urogenital finding'
    WHEN condition_concept_name = 'Pain in right foot' THEN 'Limb or extremity pain'
    WHEN condition_concept_name = 'Presbyopia' THEN 'Presbyopia'
    WHEN condition_concept_name = 'Acute cystitis' THEN 'Acute cystitis'
    WHEN condition_concept_name = 'Sepsis' THEN 'Sepsis'
    WHEN condition_concept_name = 'Altered mental status' THEN 'Altered mental status'
    WHEN condition_concept_name = 'Solitary nodule of lung' THEN 'Solitary nodule of lung'
    WHEN condition_concept_name = 'Disorder of soft tissue' THEN 'Disorder of soft tissue'
    WHEN condition_concept_name = 'Anemia' THEN 'Anemia'
    WHEN condition_concept_name = 'Abdominal distension, gaseous' THEN 'Abdominal distension, gaseous'
    WHEN condition_concept_name = 'Carpal tunnel syndrome' THEN 'Carpal tunnel syndrome'
    WHEN condition_concept_name = 'Cardiomegaly' THEN 'Cardiomegaly'
    WHEN condition_concept_name = 'Cervical radiculopathy' THEN 'Cervical radiculopathy'
    WHEN condition_concept_name = 'Cough' THEN 'Cough'
    WHEN condition_concept_name = 'No matching concept' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Prediabetes' THEN 'Prediabetes'
    WHEN condition_concept_name = 'Osteoarthritis' THEN 'Osteoarthritis '
    WHEN condition_concept_name = 'Dizziness and giddiness' THEN 'Dizziness and giddiness'
    WHEN condition_concept_name = 'Joint pain' THEN 'Joint pain'
    WHEN condition_concept_name = 'Atrial fibrillation' THEN 'Atrial fibrillation'
    WHEN condition_concept_name = 'Pruritic rash' THEN 'Pruritic rash'
    WHEN condition_concept_name = 'Chronic pain syndrome' THEN 'Chronic pain'
    WHEN condition_concept_name = 'Senile hyperkeratosis' THEN 'Senile hyperkeratosis'
    WHEN condition_concept_name = 'Lumbago with sciatica' THEN 'Back pain'
    WHEN condition_concept_name = 'Osteoporosis' THEN 'Osteoporosis'
    WHEN condition_concept_name = 'Migraine' THEN 'Migraine'
    WHEN condition_concept_name = 'Nuclear senile cataract' THEN 'Nuclear senile cataract'
    WHEN condition_concept_name = 'Chest pain' THEN 'Chest pain'
    WHEN condition_concept_name = 'Hemorrhoids' THEN 'Hemorrhoids'
    WHEN condition_concept_name = 'Hyperglycemia' THEN 'Hyperglycemia'
    WHEN condition_concept_name = 'Diaphragmatic hernia' THEN 'Diaphragmatic hernia'
    WHEN condition_concept_name = 'Lumbar radiculopathy' THEN 'Lumbar radiculopathy'
    WHEN condition_concept_name = 'Iron deficiency anemia' THEN 'Iron deficiency anemia'
    WHEN condition_concept_name = 'Headache' THEN 'Headache'
    WHEN condition_concept_name = 'Cerebral infarction' THEN 'Cerebral infarction'
    WHEN condition_concept_name = 'Anxiety disorder' THEN 'Anxiety'
    WHEN condition_concept_name = 'Tachycardia' THEN 'Tachycardia'
    WHEN condition_concept_name = 'Steatosis of liver' THEN 'Steatosis of liver'
    WHEN condition_concept_name = 'Retention of urine' THEN 'Retention of urine'
    WHEN condition_concept_name = 'Hematuria syndrome' THEN 'Hematuria syndrome'
    WHEN condition_concept_name = 'Pain in right knee' THEN 'Joint pain'
    WHEN condition_concept_name = 'Hyperlipidemia' THEN 'Hyperlipidemia'
    WHEN condition_concept_name = 'Hypothyroidism' THEN 'Hypothyroidism'
    WHEN condition_concept_name = 'Actinic keratosis' THEN 'Actinic keratosis'
    WHEN condition_concept_name = 'Obesity' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Low blood pressure' THEN 'Low blood pressure'
    WHEN condition_concept_name = 'Nasal congestion' THEN 'Nasal congestion'
    WHEN condition_concept_name = 'Alcohol dependence' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Spasm' THEN 'Spasm'
    WHEN condition_concept_name = 'Chronic kidney disease' THEN 'Chronic kidney disease'
    WHEN condition_concept_name = 'Acne' THEN 'Acne'
    WHEN condition_concept_name = 'Insomnia' THEN 'Insomnia'
    WHEN condition_concept_name = 'Acute vaginitis' THEN 'Acute vaginitis'
    WHEN condition_concept_name = 'Wrist joint pain' THEN 'Joint pain'
    WHEN condition_concept_name = 'Myopia' THEN 'Myopia'
    WHEN condition_concept_name = 'Edema' THEN 'Edema'
    WHEN condition_concept_name = 'Bipolar disorder' THEN 'Bipolar disorder'
    WHEN condition_concept_name = 'Dysuria' THEN 'Dysuria'
    WHEN condition_concept_name = 'Cardiomyopathy' THEN 'Cardiomyopathy'
    WHEN condition_concept_name = 'Morbid obesity' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Hypomagnesemia' THEN 'Hypomagnesemia'
    WHEN condition_concept_name = 'Fever' THEN 'Fever'
    WHEN condition_concept_name = 'Abdominal pain' THEN 'Abdominal pain'
    WHEN condition_concept_name = 'Viral disease' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Palpitations' THEN 'Palpitations'
    WHEN condition_concept_name = 'Leukocytosis' THEN 'Leukocytosis'
    WHEN condition_concept_name = 'Sleep apnea' THEN 'Sleep apnea'
    WHEN condition_concept_name = 'Muscle pain' THEN 'Muscle pain'
    WHEN condition_concept_name = 'Dysphagia' THEN 'Dysphagia'
    WHEN condition_concept_name = 'Pneumonia' THEN 'Pneumonia'
    WHEN condition_concept_name = 'Vomiting' THEN 'Nausea and vomiting'
    WHEN condition_concept_name = 'COVID-19' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Hip pain' THEN 'Joint pain'
    WHEN condition_concept_name = 'Backache' THEN 'Back pain'
    WHEN condition_concept_name = 'Gout' THEN 'Gout'
    WHEN condition_concept_name = 'Melena' THEN 'Melena'
    WHEN condition_concept_name = 'Eruption' THEN 'Eruption'
    WHEN condition_concept_name = 'Diarrhea' THEN 'Diarrhea'
    WHEN condition_concept_name = 'Dyspnea' THEN 'Dyspnea'
    WHEN condition_concept_name = 'Malaise' THEN 'Malaise'
    WHEN condition_concept_name = 'Allergic rhinitis due to pollen' THEN 'Allergies'
    WHEN condition_concept_name = 'Immunodeficiency disorder' THEN 'Immunodeficiency disorder'
    WHEN condition_concept_name = 'Primary malignant neoplasm of prostate' THEN 'Primary malignant neoplasm of prostate'
    WHEN condition_concept_name = 'Disorder of skin and/or subcutaneous tissue' THEN 'Disorder of skin and/or subcutaneous tissue'
    WHEN condition_concept_name = 'Open-angle glaucoma - borderline' THEN 'Open-angle glaucoma - borderline'
    WHEN condition_concept_name = 'Suspected fetal abnormality affecting management of mother' THEN 'Suspected fetal abnormality affecting management of mother'
    WHEN condition_concept_name = 'Difficulty walking' THEN 'Difficulty walking'
    WHEN condition_concept_name = 'Muscle weakness' THEN 'Muscle weakness'
    WHEN condition_concept_name = 'Hyperpigmentation of skin' THEN 'Hyperpigmentation of skin'
    WHEN condition_concept_name = 'Sensorineural hearing loss, bilateral' THEN 'Sensorineural hearing loss, bilateral'
    WHEN condition_concept_name = 'Hemangioma of skin and subcutaneous tissue' THEN 'Hemangioma of skin and subcutaneous tissue'
    WHEN condition_concept_name = 'Finding of American Society of Anesthesiologists physical status classification' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Pulmonary embolism' THEN 'Pulmonary embolism'
    WHEN condition_concept_name = 'Reduced mobility' THEN 'Reduced mobility'
    WHEN condition_concept_name = 'Combined form of senile cataract' THEN 'Combined form of senile cataract'
    WHEN condition_concept_name = 'Primary malignant neoplasm of female breast' THEN 'Primary malignant neoplasm of female breast'
    WHEN condition_concept_name = 'Traumatic AND/OR non-traumatic injury' THEN 'Injury'
    WHEN condition_concept_name = 'Acute hypoxemic respiratory failure' THEN 'Acute hypoxemic respiratory failure'
    WHEN condition_concept_name = 'Acute maxillary sinusitis' THEN 'Acute maxillary sinusitis'
    WHEN condition_concept_name = 'Amnesia' THEN 'Amnesia'
    WHEN condition_concept_name = 'Cervical spondylosis without myelopathy' THEN 'Cervical spondylosis without myelopathy'
    WHEN condition_concept_name = 'Chronic fatigue syndrome' THEN 'Chronic fatigue syndrome'
    WHEN condition_concept_name = 'Complication occurring during labor and delivery' THEN 'Pregnancy complication'
    WHEN condition_concept_name = 'Complication of pregnancy, childbirth and/or the puerperium' THEN 'Pregnancy complication'
    WHEN condition_concept_name = 'Cyst of ovary' THEN 'Cyst of ovary'
    WHEN condition_concept_name = 'Dependence on supplemental oxygen' THEN 'Dependence on supplemental oxygen'
    WHEN condition_concept_name = 'Disease of liver' THEN 'Disease of liver'
    WHEN condition_concept_name = 'Disorder of digestive system' THEN 'Disorder of digestive system'
    WHEN condition_concept_name = 'Erectile dysfunction' THEN 'Erectile dysfunction'
    WHEN condition_concept_name = 'Finding related to attentiveness' THEN 'Finding related to attentiveness'
    WHEN condition_concept_name = 'Gestation period, 8 weeks' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Gestation period, 36 weeks' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Gestation period, 37 weeks' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Gestation period, 39 weeks' THEN 'EXCLUDE'
    WHEN condition_concept_name = 'Hematuria co-occurrent and due to acute cystitis' THEN 'Hematuria co-occurrent and due to acute cystitis'
    WHEN condition_concept_name = 'Hemorrhage of rectum and anus' THEN 'Hemorrhage of rectum and anus'
    WHEN condition_concept_name = 'Injury of head' THEN 'Injury of head'
    WHEN condition_concept_name = 'Maternal AND/OR fetal condition affecting labor AND/OR delivery' THEN 'Pregnancy complication'
    WHEN condition_concept_name = 'Migraine with aura' THEN 'Migraine'
    WHEN condition_concept_name = 'Muscle strain' THEN 'Muscle strain'
    WHEN condition_concept_name = 'Panic disorder without agoraphobia' THEN 'Panic disorder without agoraphobia'
    WHEN condition_concept_name = 'Primary insomnia' THEN 'Insomnia'
    WHEN condition_concept_name = 'Pulmonary hypertension' THEN 'Pulmonary hypertension'
    WHEN condition_concept_name = 'Sleep disorder' THEN 'Sleep disorder'
    WHEN condition_concept_name = 'Supraventricular tachycardia' THEN 'Supraventricular tachycardia'
    
END AS high_level_condition

FROM pre_post_dx_final

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.770d50b8-7a31-424b-bf7c-68aef272ff08"),
    pre_post_dx_count_clean=Input(rid="ri.foundry.main.dataset.997be54c-aa13-4624-b1f6-eda716891839")
)
SELECT DISTINCT person_id, case_sex, case_apprx_age, case_race, case_ethn, case_diab_ind, case_kid_ind, case_CHF_ind, case_chronicPulm_ind
FROM pre_post_dx_count_clean

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1d25ada4-dcaf-4437-ac22-d830ccb7a5e0"),
    pre_post_med_final=Input(rid="ri.foundry.main.dataset.06f54d6d-505f-4731-b69e-d6592c2fac40")
)
SELECT DISTINCT person_id, ancestor_drug_concept_name AS ingredient, post_only_med
FROM pre_post_med_final

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7e20ea1f-74e7-4b02-b810-a3b840250c84"),
    pre_post_dx_count_clean=Input(rid="ri.foundry.main.dataset.997be54c-aa13-4624-b1f6-eda716891839")
)
SELECT condition_concept_name, condition_concept_id, count(distinct person_id) as pt_count
FROM pre_post_dx_count_clean
GROUP BY condition_concept_name, condition_concept_id
HAVING count(distinct person_id) >= (select count(distinct person_id) as tot_ct from pre_post_dx_count_clean)*0.01

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3bf4799d-a693-49c6-9b44-e3d732038f98"),
    just_cases=Input(rid="ri.foundry.main.dataset.26301130-35b1-4abc-a265-9aa9aa4afe4a")
)
WITH rollup AS
(
    SELECT  person_id,
        high_level_condition,
        patient_group, 
        -- Take the MAX count, because we are treating the condition as binary
        -- MAX(pre_dx_count) AS pre_dx_count, 
        -- MAX(post_dx_count) AS post_dx_count,
        SUM(pre_dx_count) AS pre_dx_count_sum,
        SUM(post_dx_count) AS post_dx_count_sum 
    FROM just_cases
    GROUP BY high_level_condition,
            person_id,
            patient_group
    HAVING high_level_condition IS NOT NULL
)

-- ONLY TAKE NEW CONDITIONS (FIRST OCCURRENCE IS AFTER COVID DX)
SELECT *,
        CASE
            WHEN post_dx_count_sum > pre_dx_count_sum THEN 1
            ELSE 0
        END AS greater_in_post,

        CASE
            WHEN pre_dx_count_sum = 0 AND post_dx_count_sum > 0 THEN 1
            ELSE 0
        END AS only_in_post
FROM rollup
--WHERE pre_dx_count = 0

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7c7bc43e-a928-4c5e-9ff7-03b0a7f396e6"),
    mapped_long_covid_ids=Input(rid="ri.foundry.main.dataset.5be47ee7-4fcf-4d6e-a902-85134d11d4fa")
)
SELECT person_id, data_partner_id
FROM mapped_long_covid_ids
WHERE is_pasc_patient = 1

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d435360e-1f25-48cb-8361-9998af8a51aa"),
    pre_post_med_count_clean=Input(rid="ri.foundry.main.dataset.1f93e07f-f1e0-40b6-8566-e3da6fbd3be4")
)
SELECT ancestor_drug_concept_name, ancestor_drug_concept_id, count(distinct person_id) as pt_count
FROM pre_post_med_count_clean
GROUP BY ancestor_drug_concept_name, ancestor_drug_concept_id
HAVING count(distinct person_id) >= (select count(distinct person_id) as tot_ct from pre_post_med_count_clean)*0.01

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c12045e3-2bd4-4355-af8b-8e8dd7ef1695"),
    long_covid_labels=Input(rid="ri.foundry.main.dataset.7c7bc43e-a928-4c5e-9ff7-03b0a7f396e6"),
    pre_post_dx_count_clean=Input(rid="ri.foundry.main.dataset.997be54c-aa13-4624-b1f6-eda716891839")
)
SELECT DISTINCT l.person_id, p.patient_group
FROM long_covid_labels l
INNER JOIN pre_post_dx_count_clean p
ON l.person_id = p.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ba395afa-0dd5-4933-b8d5-60bcad32f19b"),
    long_covid_labels=Input(rid="ri.foundry.main.dataset.7c7bc43e-a928-4c5e-9ff7-03b0a7f396e6"),
    pre_post_dx_count_clean=Input(rid="ri.foundry.main.dataset.997be54c-aa13-4624-b1f6-eda716891839")
)
SELECT *
FROM long_covid_labels l
LEFT ANTI JOIN pre_post_dx_count_clean p
ON l.person_id = p.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8b25c6bd-bcbd-41e4-a6d9-c9637cc8bb0b"),
    dx_above_threshold=Input(rid="ri.foundry.main.dataset.7e20ea1f-74e7-4b02-b810-a3b840250c84"),
    pre_post_dx_count_clean=Input(rid="ri.foundry.main.dataset.997be54c-aa13-4624-b1f6-eda716891839")
)
SELECT  pp.*, 
        case 
            when pre_dx_count > 0 then 0 
            else 1 
        end as post_only_dx, 
        case 
            when pre_dx_count < post_dx_count then 1 
            else 0 
        end as more_in_post
FROM pre_post_dx_count_clean pp 
INNER JOIN dx_above_threshold ds 
ON ds.condition_concept_name = pp.condition_concept_name

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.06f54d6d-505f-4731-b69e-d6592c2fac40"),
    meds_above_threshold=Input(rid="ri.foundry.main.dataset.d435360e-1f25-48cb-8361-9998af8a51aa"),
    pre_post_med_count_clean=Input(rid="ri.foundry.main.dataset.1f93e07f-f1e0-40b6-8566-e3da6fbd3be4")
)
SELECT  pp.*,
        case 
            when pre_med_count > 0 then 0 
            else 1 
        end as post_only_med,
        case 
            when pre_med_count < post_med_count then 1 
            else 0 
        end as more_in_post

FROM pre_post_med_count_clean pp
INNER JOIN meds_above_threshold m
ON pp.ancestor_drug_concept_id = m.ancestor_drug_concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e2185070-ee9e-4cc7-ae3a-4033db62f89b"),
    long_covid_labels=Input(rid="ri.foundry.main.dataset.7c7bc43e-a928-4c5e-9ff7-03b0a7f396e6")
)
SELECT COUNT(DISTINCT person_id) AS distinct_people
FROM long_covid_labels

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3278f1dc-f77c-4dcf-8f29-f666d2538c6e"),
    add_rollup=Input(rid="ri.foundry.main.dataset.ac151d2e-083e-4c6d-a35d-5eca62125ad0")
)
SELECT *
FROM add_rollup
WHERE high_level_condition != 'EXCLUDE'

@transform_pandas(
    Output(rid="ri.vector.main.execute.2e2c4d17-5f79-4262-94fb-f454981a1069"),
    pivot_just_cases=Input(rid="ri.foundry.main.dataset.5cac5244-f76c-4c70-84a5-eef337acd6e5")
)
SELECT *
FROM pivot_just_cases

