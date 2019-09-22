#!/bin/sh

cat mm_copath_patient_list-part_aa.export4dj.csv > mm_copath_patient_list.export4dj.csv
tail -n+2 mm_copath_patient_list-part_ab.export4dj.csv >> mm_copath_patient_list.export4dj.csv
tail -n+2 mm_copath_patient_list-part_ac.export4dj.csv >> mm_copath_patient_list.export4dj.csv
tail -n+2 mm_copath_patient_list-part_ad.export4dj.csv >> mm_copath_patient_list.export4dj.csv
tail -n+2 mm_copath_patient_list-part_ae.export4dj.csv >> mm_copath_patient_list.export4dj.csv
tail -n+2 mm_copath_patient_list-part_af.export4dj.csv >> mm_copath_patient_list.export4dj.csv
tail -n+2 mm_copath_patient_list-part_ag.export4dj.csv >> mm_copath_patient_list.export4dj.csv
tail -n+2 mm_copath_patient_list-part_ah.export4dj.csv >> mm_copath_patient_list.export4dj.csv

head -n-2 mm_copath_patient_list-part_aa.export4dj.xml > mm_copath_patient_list.export4dj.xml
tail -n+4 mm_copath_patient_list-part_ab.export4dj.xml | head -n-2 >> mm_copath_patient_list.export4dj.xml
tail -n+4 mm_copath_patient_list-part_ac.export4dj.xml | head -n-2 >> mm_copath_patient_list.export4dj.xml
tail -n+4 mm_copath_patient_list-part_ad.export4dj.xml | head -n-2 >> mm_copath_patient_list.export4dj.xml
tail -n+4 mm_copath_patient_list-part_ae.export4dj.xml | head -n-2 >> mm_copath_patient_list.export4dj.xml
tail -n+4 mm_copath_patient_list-part_af.export4dj.xml | head -n-2 >> mm_copath_patient_list.export4dj.xml
tail -n+4 mm_copath_patient_list-part_ag.export4dj.xml | head -n-2 >> mm_copath_patient_list.export4dj.xml
tail -n+4 mm_copath_patient_list-part_ah.export4dj.xml >> mm_copath_patient_list.export4dj.xml

