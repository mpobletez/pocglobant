from flask import Flask,request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
from flask_httpauth import HTTPBasicAuth
from sqlalchemy.sql import text
import json
from werkzeug.exceptions import HTTPException
from flask_batch import add_batch_route

app =  Flask(__name__)
app.url_map.strict_slashes = False
add_batch_route(app)

auth = HTTPBasicAuth()

USER_DATA={
     "adminUser":"password"
}

@auth.verify_password
def verify(username,password):
     if not (username and password):
          return False 
     return USER_DATA.get(username)==password

app.config['SQLALCHEMY_DATABASE_URI']='mssql+pyodbc://viewAPIUser:V13w_API_Us3r!@azdadevsql03.database.windows.net/am-da-sdb-view_migr-dq?driver=ODBC Driver 17 for SQL Server'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS']=False;

db=SQLAlchemy(app)
ma=Marshmallow(app)

class Jobs(db.Model):
    ID_AI = db.Column(db.Integer, primary_key=True) #, primary_key=True
    id = db.Column(db.Integer) #, primary_key=True
    job = db.Column(db.String(200))

    def __init__(self,id, job):
        self.id = id
        self.job = job

class JobsSchema(ma.Schema):
    class Meta:
        fields = ('id','job')

Job_schema = JobsSchema()
Jobs_schema = JobsSchema(many=True)

###### DEPARTMENT #############################

class Departments(db.Model):
    ID_AI = db.Column(db.Integer, primary_key=True) #, primary_key=True
    id = db.Column(db.Integer) #, primary_key=True
    department = db.Column(db.String(200))

    def __init__(self, id,department):
        self.id = id
        self.department = department

class DepartmentsSchema(ma.Schema):
    class Meta:
        fields = ('id','department')
        
Department_schema =  DepartmentsSchema()
Departments_schema = DepartmentsSchema(many=True)
############# END DEPARTMENTS ##############

###### EMPLOYEES #############################

class Hired_employees(db.Model):
    ID_AI = db.Column(db.Integer, primary_key=True) #, primary_key=True
    id = db.Column(db.Integer) 
    Name = db.Column(db.String(200))
    datetime = db.Column(db.String(200))
    department_id = db.Column(db.Integer)
    job_id = db.Column(db.Integer)

    def __init__(self, id,Name,datetime,department_id,job_id):
        self.id = id
        self.Name = Name
        self.datetime = datetime
        self.department_id = department_id
        self.job_id = job_id

class Hired_employeesSchema(ma.Schema):
    class Meta:
        fields = ('id','Name','datetime','department_id','job_id')
        
Hired_employee_Schema = Hired_employeesSchema()
Hired_employees_Schema = Hired_employeesSchema(many=True)
############# END EMPLOYEES ##############

@app.errorhandler(HTTPException)
def handle_bad_request(e):
    return 'bad request!', 400


@app.route('/Departments', methods=['GET'])
@auth.login_required
def GetDepartments():
       all_departments = Departments.query.all()
       result = Departments_schema.dump(all_departments)
       return jsonify(result)

@app.route('/Departments/Bulk', methods=['POST'])
@auth.login_required
def Create_Departments():
       data = list(request.json)
       try:
              for item in data:
                     id = item['id']
                     department = item['department']
                     new_department= Departments(id,department)

                     db.session.add(new_department)
                     db.session.commit()
       except KeyError:
              return 'All fields are required'
       
       return 'Departments Created Successfully'

@app.route('/Departments', methods=['POST'])
@auth.login_required
def Create_Department():
       try:
              id = request.json['id']
              department = request.json['department']
              new_department= Departments(id,department)

              db.session.add(new_department)
              db.session.commit()

              return 'Department Created Successfully'
       except KeyError:
              return 'All fields are required'
       
@app.route('/Jobs', methods=['GET'])
@auth.login_required
def GetJobs():
       all_jobs = Jobs.query.all()
       result = Jobs_schema.dump(all_jobs)
       return jsonify(result)


@app.route('/Jobs', methods=['POST'])
@auth.login_required
def Create_Job():
       try:
              id = request.json['id']
              job = request.json['job']

              new_job= Jobs(id,job)

              db.session.add(new_job)
              db.session.commit()

              return 'Job Created Successfully'
       except KeyError:
              return 'All fields are required'
       

@app.route('/Jobs/Bulk', methods=['POST'])
@auth.login_required
def Create_Jobs():
       data = list(request.json)
       try:
              for item in data:
                     id = item['id']
                     job = item['job']

                     new_job= Jobs(id,job)

                     db.session.add(new_job)
                     db.session.commit()

       except KeyError:
              return 'All fields are required'
       
       return 'Jobs Created Successfully'
       
@app.route('/Employees', methods=['POST'])
@auth.login_required
def Create_Employees():
       try:
              id = request.json['id']
              Name = request.json['Name']
              datetime = request.json['datetime']
              department_id = request.json['department_id']
              job_id = request.json['job_id']

              new_employee= Hired_employees(id,Name,datetime, department_id,job_id)

              db.session.add(new_employee)
              db.session.commit()

              return 'Employee Created Successfully'
       except KeyError:
              return 'All fields are required'
       
@app.route('/Employees/Bulk', methods=['POST'])
@auth.login_required
def Create_Employees_bulk():
       data = list(request.json)
       try:
              for item in data:
                     id = item['id']
                     Name = item['Name']
                     datetime = item['datetime']
                     department_id = item['department_id']
                     job_id = item['job_id']
                     new_employee= Hired_employees(id,Name,datetime, department_id,job_id)
                     db.session.add(new_employee)
                     db.session.commit()
              return 'Employees Created Successfully'
       except KeyError:
              return 'All fields are required'
       
@app.route('/Employees', methods=['GET'])
@auth.login_required
def GetEmployees():
       all_Employees = Hired_employees.query.all()
       result = Hired_employees_Schema.dump(all_Employees)
       return jsonify(result)

@app.route('/Query1', methods=['GET'])
def GetQuery1():
       result = db.session.execute(text('select department,job, Q1=sum(case when Quartile=1 then 1 else 0 end ),Q2=sum(case when Quartile=2 then 1 else 0 end ),Q3=sum(case when Quartile=3 then 1 else 0 end ),Q4=sum(case when Quartile=4 then 1 else 0 end ) from(select b.department,c.job, NTILE(4) OVER (PARTITION BY month(convert(datetime,[datetime])) ORDER BY department) AS Quartile from hired_employees a inner join departments b on a.department_id=b.id inner join jobs c on a.job_id=c.id where year(convert(datetime,datetime))=2021) a group by department,job'))
       #jsonify(result)
       #return jsonify(list(result))
       return json.dumps(list(result), default=str)

@app.route('/Query2', methods=['GET'])
def GetQuery2():
       result = db.session.execute(text('declare @val int select @val= avg(count) from (select b.department,count(*) as count from hired_employees a inner join departments b on a.department_id=b.id where year(convert(datetime,datetime))=2021 group by b.department)a select b.id,b.department, count(*)  as Hired from hired_employees a inner join departments b on a.department_id=b.id where year(convert(datetime,datetime))=2021 group by b.id,b.department having count(*) > @val'))
       #jsonify(result)
       #return jsonify(list(result))
       return json.dumps(list(result), default=str)



if __name__=='__main__':
        with app.app_context():
            db.create_all()
            app.run(port=4000,debug=True, threaded=True)