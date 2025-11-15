-- Lovato_Tech Premium Guardian/Parent Portal schema (safe to run multiple times)
-- These tables are optional; the portal works without them if not used.

-- Guardians table (for future: if you want parent accounts)
CREATE TABLE IF NOT EXISTS guardians (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(150) NOT NULL,
  email VARCHAR(190) NULL,
  phone VARCHAR(40) NULL,
  password_hash VARCHAR(255) NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_guardian_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Map guardians to students (many-to-many)
CREATE TABLE IF NOT EXISTS guardian_students (
  id INT AUTO_INCREMENT PRIMARY KEY,
  guardian_id INT NOT NULL,
  student_id INT NOT NULL,
  relationship VARCHAR(40) NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_guardian_student (guardian_id, student_id),
  INDEX idx_gs_student (student_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Announcements (if you prefer not to reuse newsletters)
CREATE TABLE IF NOT EXISTS announcements (
  id INT AUTO_INCREMENT PRIMARY KEY,
  school_id INT NULL,
  title VARCHAR(200) NOT NULL,
  body LONGTEXT NOT NULL,
  category VARCHAR(40) NOT NULL DEFAULT 'announcement',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_ann_school (school_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Grades (simple schema for performance tracker)
CREATE TABLE IF NOT EXISTS grades (
  id INT AUTO_INCREMENT PRIMARY KEY,
  student_id INT NOT NULL,
  year INT NOT NULL,
  term INT NOT NULL,
  subject VARCHAR(100) NOT NULL,
  mark DECIMAL(5,2) NOT NULL,
  recorded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_grades_student_term (student_id, year, term)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Guardian in-app messages (if not already present via student_portal)
CREATE TABLE IF NOT EXISTS guardian_messages (
  id INT AUTO_INCREMENT PRIMARY KEY,
  student_id INT NOT NULL,
  school_id INT NULL,
  name VARCHAR(150) NULL,
  email VARCHAR(190) NULL,
  phone VARCHAR(40) NULL,
  category VARCHAR(40) NULL,
  subject VARCHAR(190) NULL,
  message TEXT NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_gm_student (student_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Calendar events (dedicated table)
CREATE TABLE IF NOT EXISTS calendar_events (
  id INT AUTO_INCREMENT PRIMARY KEY,
  school_id INT NULL,
  title VARCHAR(200) NOT NULL,
  description TEXT NULL,
  category VARCHAR(40) NULL,
  start_date DATE NOT NULL,
  end_date DATE NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_events_school (school_id),
  INDEX idx_events_dates (start_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
