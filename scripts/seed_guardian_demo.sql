-- Demo data for Guardian/Parent login flow
-- Adjust database name if needed

-- Create school if missing
INSERT INTO schools (code, name)
SELECT 'demo-school', 'Demo School'
WHERE NOT EXISTS (SELECT 1 FROM schools WHERE code='demo-school');

-- Link a student with last name 'Doe' and admission_no 'ADM001'
INSERT INTO students (name, admission_no, class_name, balance, school_id)
SELECT 'Mary Doe', 'ADM001', 'Grade 6', 25000, s.id
FROM schools s
WHERE s.code='demo-school' AND NOT EXISTS (
  SELECT 1 FROM students st WHERE st.admission_no='ADM001' AND st.school_id=s.id
);

-- Optional: clear any prior portal password hash to test first-time login -> auto-hash
UPDATE students st
JOIN schools s ON s.id=st.school_id AND s.code='demo-school'
SET st.portal_password_hash=NULL
WHERE st.admission_no='ADM001';
