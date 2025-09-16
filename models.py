from datetime import datetime
from utils import db

# Student model
class Student(db.Model):
    __tablename__ = "students"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    admission_no = db.Column(db.String(50), unique=True, nullable=False)
    student_class = db.Column("class", db.String(50), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    # Relationship (a student can have many payments)
    payments = db.relationship("Payment", backref="student", lazy=True)

    def __repr__(self):
        return f"<Student {self.name} - {self.admission_no}>"


# Fee structure model
class Fee(db.Model):
    __tablename__ = "fees"

    id = db.Column(db.Integer, primary_key=True)
    student_class = db.Column("class", db.String(50), nullable=False)
    amount = db.Column(db.Numeric(10, 2), nullable=False)
    term = db.Column(db.String(20), nullable=False)
    year = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    # Relationship (a fee structure can have many payments)
    payments = db.relationship("Payment", backref="fee", lazy=True)

    def __repr__(self):
        return f"<Fee {self.student_class} - {self.term} {self.year}>"


# Payment model
class Payment(db.Model):
    __tablename__ = "payments"

    id = db.Column(db.Integer, primary_key=True)
    student_id = db.Column(db.Integer, db.ForeignKey("students.id"), nullable=False)
    fee_id = db.Column(db.Integer, db.ForeignKey("fees.id"), nullable=False)
    amount_paid = db.Column(db.Numeric(10, 2), nullable=False)
    payment_date = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<Payment StudentID={self.student_id} FeeID={self.fee_id} Amount={self.amount_paid}>"
