from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()


class Student(db.Model):
    __tablename__ = 'students'
    __table_args__ = (
        db.UniqueConstraint('school_id', 'admission_no', name='uq_students_school_admission'),
    )

    id = db.Column(db.Integer, primary_key=True)
    # Store in DB column 'admission_no' but expose as attribute 'regNo' for compatibility
    regNo = db.Column('admission_no', db.String(50), nullable=False)
    school_id = db.Column(db.Integer, index=True, nullable=True)  # Multi-tenant scoping for admission numbers
    name = db.Column(db.String(100), nullable=False)
    class_name = db.Column(db.String(50))
    phone = db.Column(db.String(20))
    balance = db.Column(db.Numeric(10, 2), default=0.00)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    fees = db.relationship('Fee', backref='student', cascade="all, delete-orphan")
    payments = db.relationship('Payment', backref='student', cascade="all, delete-orphan")

    def __repr__(self):
        return f'<Student {self.name} ({self.regNo})>'


class Fee(db.Model):
    __tablename__ = 'fees'

    id = db.Column(db.Integer, primary_key=True)
    student_id = db.Column(db.Integer, db.ForeignKey('students.id'), nullable=False)
    amount_due = db.Column(db.Numeric(10, 2), nullable=False)
    due_date = db.Column(db.Date, nullable=False)

    def __repr__(self):
        return f'<Fee StudentID={self.student_id} Due={self.amount_due}>'


class Payment(db.Model):
    __tablename__ = 'payments'

    id = db.Column(db.Integer, primary_key=True)
    student_id = db.Column(db.Integer, db.ForeignKey('students.id'), nullable=False)
    amount_paid = db.Column(db.Numeric(10, 2), nullable=False)
    payment_date = db.Column(db.Date, nullable=False)

    def __repr__(self):
        return f'<Payment StudentID={self.student_id} Paid={self.amount_paid}>'
