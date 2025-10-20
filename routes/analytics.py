@app.route('/analytics')
def analytics():
    total_students = Student.query.count()
    total_collected = db.session.query(db.func.sum(Payment.amount)).scalar() or 0
    total_balance = db.session.query(db.func.sum(Student.balance)).scalar() or 0
    recent_payments = Payment.query.order_by(Payment.payment_date.desc()).limit(10).all()

    # Class summary (class name, count, total balance)
    class_summary = (
        db.session.query(Student.class_name, db.func.count(Student.id), db.func.sum(Student.balance))
        .group_by(Student.class_name)
        .all()
    )

    # Monthly Fee Collection Trends
    monthly_data = (
        db.session.query(
            db.func.strftime('%Y-%m', Payment.payment_date).label('month'),
            db.func.sum(Payment.amount).label('total')
        )
        .group_by('month')
        .order_by('month')
        .all()
    )

    months = [m[0] for m in monthly_data]
    totals = [float(m[1]) for m in monthly_data]

    return render_template(
        'analytics.html',
        total_students=total_students,
        total_collected=total_collected,
        total_balance=total_balance,
        recent_payments=recent_payments,
        class_summary=class_summary,
        months=months,
        totals=totals
    )
