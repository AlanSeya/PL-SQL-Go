package main

import (
	 "database/sql"
	 "fmt"
	 "log"
	_"github.com/lib/pq"
	"encoding/json"
	bolt "go.etcd.io/bbolt"
	"strconv"
	"time"
)

type Cliente struct{
	Nrocliente int
	Nombre string
	Apellido string
	Domicilio string
	Telefono string
}

type Tarjeta struct{
	Nrotarjeta string
	Nrocliente int
	Validadesde string
	Validahasta string
	Codseguridad string
	Limitecompra float32
	Estado string
}

type Comercio struct{
	Nrocomercio int
	Nombre string
	Domicilio string
	Codigopostal string
	Telefono string
}

type Compra struct{
	Nrooperacion int64
	Nrotarjeta string
	Nrocomercio int
	Fecha time.Time
	Monto float32
	Pagado bool
}

type Rechazo struct{
	Nrorechazo int64
	Nrotarjeta string
	Nrocomercio int
	Fecha time.Time
	Monto float32
	Motivo string
}

type Cierre struct{
	Año int
	Mes int
	Terminacion int
	Fechainicio time.Time
	Fechacierre time.Time
	Fechavto time.Time
}

type Cabecera struct {
	NroResumen   int64
	Nombre       string
	Apellido     string
	Domicilio    string
	NroTarjeta   string
	Desde        time.Time
	Hasta        time.Time
	Vence        time.Time
	Total        float32
}

type Detalle struct {
	NroResumen     int64
	NroLinea       int
	Fecha          time.Time
	NombreComercio string
	Monto          float32
}

type Alerta struct {
	NroAlerta   int64
	NroTarjeta  string
	Fecha       time.Time
	NroRechazo  int
	CodAlerta   int
	Descripcion string
}

type Consumo struct {
	NroTarjeta     string
	CodSeguridad   string
	NroComercio    int
	Monto          float32
}

func main() {
	 db, err := sql.Open("postgres", "user=postgres host=localhost dbname=bd2 sslmode=disable")
	 if err != nil {
	 log.Fatal(err)
	 }
		defer db.Close()
		
		var check = true
		
	    dropBD()
		conectarBD()
	  for check {
		  menu()
		  opcion := leerOpcion()

		switch opcion {
		case 1:
			crearBD()
			fmt.Println("Has creado la Base de datos")
			fmt.Println("---------------------------\n")
		case 2:
			crearTablas()
			fmt.Println("Se han creado las tablas")
			fmt.Println("-------------------------\n")
		case 3:
			agregarPkFk()
			authCompraSP()
			fmt.Println("Se han insertado las Pk y Fk y creado los Stored Procedures")
			fmt.Println("------------------------------------------------------------\n")
		case 4:
			insertarComercios()
			insertarClientes()
			insertarTarjetas()
			insertarCierres()
			emitirResumen()
			alertasAClientes2()
			suspenderTarjetaExcesoLimite()
			alertaRechazo()
			
			fmt.Println("Se han insertado datos")
			fmt.Println("-----------------------\n")
		case 5:
			borrarPKFK()
			fmt.Println("Se han borrado las Pk y FK.")
			fmt.Println("----------------------------\n")	
		case 6:	
			// Espacio para correr tests.
			//insertarConsumos()
			//insertarCompra()
			probarConsumos()
			exportarABoltDB()
		case 0:
			fmt.Println("Ha salido del programa")
			fmt.Println("----------------------\n")
			check = false
		default:
			fmt.Println("Opción inválida. Intentá nuevamente.")
			fmt.Println("------------------------------------\n")

			}
		}
}

func menu() {
	fmt.Printf("Menú de opciones:\n")
	fmt.Printf("1. Crear Base de Datos\n")
	fmt.Printf("2. Crear tablas\n")
	fmt.Printf("3. Cargar Pk y Fk\n")
	fmt.Printf("4. Cargar datos\n")
	fmt.Printf("5. Borrar PK y FK\n")
	fmt.Printf("6. Correr tests y exportar a BoltDB\n")
	fmt.Printf("0. Salir\n")
	fmt.Printf("\n")
}

func leerOpcion() int {
	var opcion int
	fmt.Printf("Selecciona una opción: ")
	fmt.Scanln(&opcion)
	fmt.Printf("\n")
	return opcion
}

func conectarBD() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=postgres sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}

func dropBD() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=postgres sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`drop database if exists bd2`)

	if err != nil {
		log.Fatal(err)
	}
}

func crearBD() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=postgres sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`create database bd2`)

	if err != nil {
		log.Fatal(err)
	}
}

func crearTablas() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=bd2 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
	create table cliente(
		nrocliente int,
		nombre text,
		apellido text,
		domicilio text,
		telefono char(12)
	);

	create table tarjeta(
		nrotarjeta char(16),
		nrocliente int,
		validadesde char(6),
		validahasta char(6),
		codseguridad char(4),
		limitecompra decimal(8,2),
		estado char(10)
	);

	create table comercio(
		nrocomercio int,
		nombre text,
		domicilio text,
		codigopostal char(8),
		telefono char(12)
	);

	create table compra(
		nrooperacion serial,
		nrotarjeta char(16),
		nrocomercio int,
		fecha timestamp,
		monto decimal(7,2),
		pagado boolean
	);

	create table rechazo(
		nrorechazo serial,
		nrotarjeta char(16),
		nrocomercio int,
		fecha timestamp,
		monto decimal(7,2),
		motivo text
	);

	create table cierre(
		año int,
		mes int,
		terminacion int,
		fechainicio date,
		fechacierre date,
		fechavto date
	);

	create table cabecera(
		nroresumen serial,
		nombre text,
		apellido text,
		domicilio text,
		nrotarjeta char(16),
		desde date,
		hasta date,
		vence date,
		total decimal(8,2)
	);

	create table detalle(
		nroresumen int,
		nrolinea int,
		fecha date,
		nombrecomercio text,
		monto decimal(7,2)
	);

	create table alerta(
		nroalerta serial,
		nrotarjeta char(16),
		fecha timestamp,
		nrorechazo int,
		codalerta int,
	descripcion text
	);
	
	create table consumo(
		nrotarjeta char(16),
		codseguridad char(4),
		nrocomercio int,
		monto decimal (7,2)
		);
		`)
		if err != nil {
		log.Fatal(err)
		}

}

func agregarPkFk() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=bd2 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`
		alter table cliente add constraint nrocliente_pk primary key (nrocliente);
		alter table tarjeta add constraint nrotarjeta_pk primary key (nrotarjeta);
		alter table tarjeta add constraint tarjeta_nrocliente_fk foreign key (nrocliente) references cliente(nrocliente);
		alter table comercio add constraint nrocomercio_pk primary key (nrocomercio);
		alter table compra add constraint nrooperacion_pk primary key (nrooperacion);
		alter table compra add constraint compra_nrotarjeta_fk foreign key (nrotarjeta) references tarjeta(nrotarjeta);
		alter table compra add constraint compra_nrocomercio_fk foreign key (nrocomercio) references comercio(nrocomercio);
		alter table rechazo add constraint nrorechazo_pk primary key (nrorechazo);
		alter table rechazo add constraint rechazo_nrocomercio_fk foreign key (nrocomercio) references comercio(nrocomercio);
		alter table cierre add constraint cierre_pk primary key (año,mes,terminacion);
		alter table cabecera add constraint nroresumen_pk primary key (nroresumen);
		alter table cabecera add constraint cabecera_nrotarjeta_fk foreign key (nrotarjeta) references tarjeta(nrotarjeta);
		alter table detalle add constraint detalle_pk primary key (nroresumen,nrolinea);
		alter table detalle add constraint detalle_nroresumen_fk foreign key (nroresumen) references cabecera(nroresumen);
		alter table alerta add constraint nroalerta_pk primary key (nroalerta);
	`)
	if err != nil {
		log.Fatal(err)
		}
}

func insertarClientes() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=bd2 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
		insert into cliente values(1, 'Linus', 'Torvalds','Balcarce 50',541163548716);
		insert into cliente values(2, 'Tim', 'Turing','San Martín 27',541163684359);
		insert into cliente values(3, 'Alan ', 'Torvalds','Cerrito 628',541167845965);
		insert into cliente values(4, 'Ada', 'Lovelace','Junín 1760',541168932471);
		insert into cliente values(5, 'Grace', 'Hopper','Avenida de Mayo 1370',541176340196);
		insert into cliente values(6, 'Dennis', 'Ritchie','Bolívar 65',54112017846);
		insert into cliente values(7, 'Bjarne', 'Stroustrup','Av. del Libertador 1473',54113482046);
		insert into cliente values(8, 'Guido', 'van Rossum','Av. Entre Ríos 51',54113008905);
		insert into cliente values(9, 'James', 'Gosling','Av. de Mayo 825',541142016872);
		insert into cliente values(10, 'Sergey', 'Brin','Av. Córdoba 550',541178452162);
		insert into cliente values(11, 'Bill', 'Gates','Juana Manso 500',541164219785);
		insert into cliente values(12, 'Steve', 'Jobs','Av. Tristán Achával Rodríguez 1550',541116324597);
		insert into cliente values(13, 'Richard', 'Stallman','Av. de Mayo 575',541163257942);
		insert into cliente values(14, 'Ken', 'Thompson','Libertad 815',541163016982);
		insert into cliente values(15, 'John', 'Carmack','Defensa 1600',541179635468);
		insert into cliente values(16, 'Niklaus', 'Wirth','Riobamba 750',541112498763);
		insert into cliente values(17, 'Martin', ' Fowler','Perú 272',541101284937);
		insert into cliente values(18, 'Andrew', 'Tanenbaum','Pasaje San Lorenzo 380',541162368548);
		insert into cliente values(19, 'Bob', 'Kahn',' Lafinur 2988',541171032698);
		insert into cliente values(20, 'Donald', ' Knuth','Suipacha 1422',541184751698);
	`)
	if err != nil {
		log.Fatal(err)
		}
}

func insertarComercios() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=bd2 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	_, err = db.Exec(`
	insert into comercio values(1,'TechZone', 'Dardo Rocha 1342', 'B1640FSL',541152460424);
	insert into comercio values(2,'SmartLab', 'Av. Hipólito Yrigoyen 2400', 'B1646CGU',541152548390);
	insert into comercio values(3,'Digital Mind', 'Juan N.Madero 1258', 'B1646DXL',541152738199);
	insert into comercio values(4,'TechHub', 'Blanco Escalada 564', 'B1609EES',541152371378);
	insert into comercio values(5,'FutureTech', 'Valentin Vergara 80', 'B1609DLB',541152194197);
	insert into comercio values(6,'InnoSpace', 'Av. Pres. Perón 2698' ,'B1644CYQ',541153523782);
	insert into comercio values(7,'CreativaTech', 'Cuyo 1926', 'B1640GHX',541152460476);
	insert into comercio values(8,'Mundo Digital', 'Arenales 1825', 'B1640BGK',541152742481);
	insert into comercio values(9,'IdeaBox', 'Corrientes 1297', 'B1640HOC',541152461341);
	insert into comercio values(10,'TechWave', 'Av. del Libertador 2803', 'B1636DSH',541152799760);
	insert into comercio values(11,'VisionTec', 'Av. Maipú 2767', 'B1636AAH',541152357164);
	insert into comercio values(12,'IdeaWorks', 'Av. Maipú 1050', 'B1602AAX',541152738199);
	insert into comercio values(13,'EmprendeTec', 'Av. Juan Bautista Justo 8900', 'C1408AKR',541152356193);
	insert into comercio values(14,'Digital Flow', 'Av. Juan Bautista Alberdi 6101', 'C1440AAM',541152461341);
	insert into comercio values(15,'Nube Tecnologica', 'Av. del Libertador 7112', 'C1429BMR',541152371378);
	insert into comercio values(16,'InnovaLabs', 'Av. Triunvirato 4680', 'C1431FBW',541153523782);
	insert into comercio values(17,'Creative Code', 'Av. Pte. J. D. Perón 1522', 'B1663GHR',541152742481);
	insert into comercio values(18,'TechSpot', 'Av. Pte. J. D. Perón 989', 'B1662ASJ',541152799736);
	insert into comercio values(19,'Tech Genius', 'Av. Cabildo 2254', 'C1428AAR',541152357164);
	insert into comercio values(20,'Neo Labs', 'Av. Elcano 3240', 'C1426EJQ',541152548390);
	`)
	if err != nil {
		log.Fatal(err)
		}
}

func insertarTarjetas() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=bd2 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	
	_, err = db.Exec(`
		insert into tarjeta (nrotarjeta, nrocliente, validadesde, validahasta, codseguridad, limitecompra, estado)
		values ('4485416422706071', 1, '042019', '052026', '1264', '5000', 'vigente'),
			   ('4485444643101179', 2, '022018', '012027', '4831', '4000', 'vigente'),
			   ('4532955337059731', 3, '072020', '022025', '8004', '6000', 'vigente'),
			   ('5594873581008639', 4, '092018', '042026', '8074', '7000', 'vigente'),
			   ('5367984198314628', 5, '112017', '052025', '3535', '3000', 'vigente'),
			   ('5364052507608396', 6, '062015', '022022', '8716', '5000', 'expirada'),
			   ('4883701190146057', 7, '082021', '022026', '8675', '6000', 'vigente'),
			   ('4485671178867960', 8, '122020', '062024', '3293', '4000', 'vigente'),
			   ('4024007162422587', 9, '072018', '012028', '9062', '7000', 'vigente'),
			   ('5412506730140339', 10, '032016', '052026', '8524', '3000', 'vigente'),
			   ('5183364390233946', 11, '062019', '102028', '2364', '4000', 'vigente'),
			   ('5356240173675977', 12, '062020', '022028', '6494', '2000', 'vigente'),
			   ('4532279835725937', 13, '042021', '112026', '2278', '6000', 'vigente'),
			   ('4556487549198280', 14, '042022', '032026', '2605', '5000', 'vigente'),
			   ('4485118765801796', 15, '122021', '022027', '8143', '4000', 'vigente'),
			   ('5185491637214914', 16, '072019', '042025', '9156', '3000', 'vigente'),
			   ('5380806308794220', 17, '022022', '082029', '5123', '6000', 'vigente'),
			   ('5338845734673973', 18, '012015', '022027', '9901', '4000', 'vigente'),
			   ('4716515488257399', 19, '062021', '122029', '2612', '7000', 'vigente'),
			   ('4532938327362968', 20, '032022', '072028', '3630', '4000', 'vigente')
	`)
	if err != nil {
		log.Fatal(err)
	}
}
		
func insertarCierres() {
		db, err := sql.Open("postgres", "user=postgres host=localhost dbname=bd2 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	
	_, err = db.Exec(`
		insert into cierre (año, mes, terminacion, fechainicio, fechacierre, fechavto)
	values
		(2023, 1, 0,'2022-12-11','2023-01-10','2023-01-20'),
		(2023, 1, 1,'2022-12-02','2023-01-01','2023-01-11'),
		(2023, 1, 2,'2022-12-03','2023-01-02','2023-01-12'),
		(2023, 1, 3,'2022-12-04','2023-01-03','2023-01-13'),
		(2023, 1, 4,'2022-12-05','2023-01-04','2023-01-14'),
		(2023, 1, 5,'2022-12-06','2023-01-05','2023-01-15'),
		(2023, 1, 6,'2022-12-05','2023-01-06','2023-01-16'),
		(2023, 1, 7,'2022-12-06','2023-01-07','2023-01-17'),
		(2023, 1, 8,'2022-12-07','2023-01-08','2023-01-18'),
		(2023, 1, 9,'2022-12-08','2023-01-09','2023-01-19'),
		(2023, 2, 0,'2023-01-11','2023-02-10','2023-02-20'),
		(2023, 2, 1,'2023-01-02','2023-02-01','2023-02-11'),
		(2023, 2, 2,'2023-01-03','2023-02-02','2023-02-12'),
		(2023, 2, 3,'2023-01-04','2023-02-03','2023-02-13'),
		(2023, 2, 4,'2023-01-05','2023-02-04','2023-02-14'),
		(2023, 2, 5,'2023-01-06','2023-02-05','2023-02-15'),
		(2023, 2, 6,'2023-01-05','2023-02-06','2023-02-16'),
		(2023, 2, 7,'2023-01-06','2023-02-07','2023-02-17'),
		(2023, 2, 8,'2023-01-07','2023-02-08','2023-02-18'),
		(2023, 2, 9,'2023-01-08','2023-02-09','2023-02-19'),
		(2023, 3, 0,'2023-02-11','2023-03-10','2023-03-20'),
		(2023, 3, 1,'2023-02-02','2023-03-01','2023-03-11'),
		(2023, 3, 2,'2023-02-03','2023-03-02','2023-03-12'),
		(2023, 3, 3,'2023-02-04','2023-03-03','2023-03-13'),
		(2023, 3, 4,'2023-02-05','2023-03-04','2023-03-14'),
		(2023, 3, 5,'2023-02-06','2023-03-05','2023-03-15'),
		(2023, 3, 6,'2023-02-05','2023-03-06','2023-03-16'),
		(2023, 3, 7,'2023-02-06','2023-03-07','2023-03-17'),
		(2023, 3, 8,'2023-02-07','2023-03-08','2023-03-18'),
		(2023, 3, 9,'2023-02-08','2023-03-09','2023-03-19'),
		(2023, 4, 0,'2023-03-11','2023-04-10','2023-04-20'),
		(2023, 4, 1,'2023-03-02','2023-04-01','2023-04-11'),
		(2023, 4, 2,'2023-03-03','2023-04-02','2023-04-12'),
		(2023, 4, 3,'2023-03-04','2023-04-03','2023-04-13'),
		(2023, 4, 4,'2023-03-05','2023-04-04','2023-04-14'),
		(2023, 4, 5,'2023-03-06','2023-04-05','2023-04-15'),
		(2023, 4, 6,'2023-03-05','2023-04-06','2023-04-16'),
		(2023, 4, 7,'2023-03-06','2023-04-07','2023-04-17'),
		(2023, 4, 8,'2023-03-07','2023-04-08','2023-04-18'),
		(2023, 4, 9,'2023-03-08','2023-04-09','2023-04-19'),
		(2023, 5, 0,'2023-04-11','2023-05-10','2023-05-20'),
		(2023, 5, 1,'2023-04-02','2023-05-01','2023-05-11'),
		(2023, 5, 2,'2023-04-03','2023-05-02','2023-05-12'),
		(2023, 5, 3,'2023-04-04','2023-05-03','2023-05-13'),
		(2023, 5, 4,'2023-04-05','2023-05-04','2023-05-14'),
		(2023, 5, 5,'2023-04-06','2023-05-05','2023-05-15'),
		(2023, 5, 6,'2023-04-05','2023-05-06','2023-05-16'),
		(2023, 5, 7,'2023-04-06','2023-05-07','2023-05-17'),
		(2023, 5, 8,'2023-04-07','2023-05-08','2023-05-18'),
		(2023, 5, 9,'2023-04-08','2023-05-09','2023-05-19'),
		(2023, 6, 0,'2023-04-11','2023-06-10','2023-06-20'),
		(2023, 6, 1,'2023-05-02','2023-06-01','2023-06-11'),
		(2023, 6, 2,'2023-05-03','2023-06-02','2023-06-12'),
		(2023, 6, 3,'2023-05-04','2023-06-03','2023-06-13'),
		(2023, 6, 4,'2023-05-05','2023-06-04','2023-06-14'),
		(2023, 6, 5,'2023-05-06','2023-06-05','2023-06-15'),
		(2023, 6, 6,'2023-05-05','2023-06-06','2023-06-16'),
		(2023, 6, 7,'2023-05-06','2023-06-07','2023-06-17'),
		(2023, 6, 8,'2023-05-07','2023-06-08','2023-06-18'),
		(2023, 6, 9,'2023-05-08','2023-06-09','2023-06-19'),
		(2023, 7, 0,'2023-06-11','2023-07-10','2023-07-20'),
		(2023, 7, 1,'2023-06-02','2023-07-01','2023-07-11'),
		(2023, 7, 2,'2023-06-03','2023-07-02','2023-07-12'),
		(2023, 7, 3,'2023-06-04','2023-07-03','2023-07-13'),
		(2023, 7, 4,'2023-06-05','2023-07-04','2023-07-14'),
		(2023, 7, 5,'2023-06-06','2023-07-05','2023-07-15'),
		(2023, 7, 6,'2023-06-05','2023-07-06','2023-07-16'),
		(2023, 7, 7,'2023-06-06','2023-07-07','2023-07-17'),
		(2023, 7, 8,'2023-06-07','2023-07-08','2023-07-18'),
		(2023, 7, 9,'2023-06-08','2023-07-09','2023-07-19'),
		(2023, 8, 0,'2023-07-11','2023-08-10','2023-08-20'),
		(2023, 8, 1,'2023-07-02','2023-08-01','2023-08-11'),
		(2023, 8, 2,'2023-07-03','2023-08-02','2023-08-12'),
		(2023, 8, 3,'2023-07-04','2023-08-03','2023-08-13'),
		(2023, 8, 4,'2023-07-05','2023-08-04','2023-08-14'),
		(2023, 8, 5,'2023-07-06','2023-08-05','2023-08-15'),
		(2023, 8, 6,'2023-07-05','2023-08-06','2023-08-16'),
		(2023, 8, 7,'2023-07-06','2023-08-07','2023-08-17'),
		(2023, 8, 8,'2023-07-07','2023-08-08','2023-08-18'),
		(2023, 8, 9,'2023-07-08','2023-08-09','2023-08-19'),
		(2023, 9, 0,'2023-08-11','2023-09-10','2023-09-20'),
		(2023, 9, 1,'2023-08-02','2023-09-01','2023-09-11'),
		(2023, 9, 2,'2023-08-03','2023-09-02','2023-09-12'),
		(2023, 9, 3,'2023-08-04','2023-09-03','2023-09-13'),
		(2023, 9, 4,'2023-08-05','2023-09-04','2023-09-14'),
		(2023, 9, 5,'2023-08-06','2023-09-05','2023-09-15'),
		(2023, 9, 6,'2023-08-05','2023-09-06','2023-09-16'),
		(2023, 9, 7,'2023-08-06','2023-09-07','2023-09-17'),
		(2023, 9, 8,'2023-08-07','2023-09-08','2023-09-18'),
		(2023, 9, 9,'2023-08-08','2023-09-09','2023-09-19'),
		(2023, 10, 0,'2023-09-11','2023-10-10','2023-10-20'),
		(2023, 10, 1,'2023-09-02','2023-10-01','2023-10-11'),
		(2023, 10, 2,'2023-09-03','2023-10-02','2023-10-12'),
		(2023, 10, 3,'2023-09-04','2023-10-03','2023-10-13'),
		(2023, 10, 4,'2023-09-05','2023-10-04','2023-10-14'),
		(2023, 10, 5,'2023-09-06','2023-10-05','2023-10-15'),
		(2023, 10, 6,'2023-09-05','2023-10-06','2023-10-16'),
		(2023, 10, 7,'2023-09-06','2023-10-07','2023-10-17'),
		(2023, 10, 8,'2023-09-07','2023-10-08','2023-10-18'),
		(2023, 10, 9,'2023-09-08','2023-10-09','2023-10-19'),
		(2023, 11, 0,'2023-10-11','2023-11-10','2023-11-20'),
		(2023, 11, 1,'2023-10-02','2023-11-01','2023-11-11'),
		(2023, 11, 2,'2023-10-03','2023-11-02','2023-11-12'),
		(2023, 11, 3,'2023-10-04','2023-11-03','2023-11-13'),
		(2023, 11, 4,'2023-10-05','2023-11-04','2023-11-14'),
		(2023, 11, 5,'2023-10-06','2023-11-05','2023-11-15'),
		(2023, 11, 6,'2023-10-05','2023-11-06','2023-11-16'),
		(2023, 11, 7,'2023-10-06','2023-11-07','2023-11-17'),
		(2023, 11, 8,'2023-10-07','2023-11-08','2023-11-18'),
		(2023, 11, 9,'2023-10-08','2023-11-09','2023-11-19'),
		(2023, 12, 0,'2023-11-11','2023-12-10','2023-12-20'),
		(2023, 12, 1,'2023-11-02','2023-12-01','2023-12-11'),
		(2023, 12, 2,'2023-11-03','2023-12-02','2023-12-12'),
		(2023, 12, 3,'2023-11-04','2023-12-03','2023-12-13'),
		(2023, 12, 4,'2023-11-05','2023-12-04','2023-12-14'),
		(2023, 12, 5,'2023-11-06','2023-12-05','2023-12-15'),
		(2023, 12, 6,'2023-11-05','2023-12-06','2023-12-16'),
		(2023, 12, 7,'2023-11-06','2023-12-07','2023-12-17'),
		(2023, 12, 8,'2023-11-07','2023-12-08','2023-12-18'),
		(2023, 12, 9,'2023-11-08','2023-12-09','2023-12-19');			   
	`)
	if err != nil {
		log.Fatal(err)
	}
}

func insertarConsumos() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=bd2 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	_, err = db.Exec(`
	insert into consumo( nrotarjeta, codseguridad, nrocomercio, monto) 
	values('4883701190146057','8675', 2, 1750.20 ),
		  ('4532279835725937','2278', 13, 57.50),
		  ('4485671627867969','5769', 16, 1200.00),
		  ('5183364390233946','1234', 1, 100.20),
		  ('5412506730140339','8524', 8, 375.00),
		  ('5356240173675977','6494', 10, 2100.50),
		  ('5364052507608396','8716', 11, 520.10),
		  ('4556487549198280','2605', 19, 626.66),
		  ('5183364390233946','2664', 7, 150.00),
		  ('5364052507608396','8716', 2, 120.50),/*expirada*/
		  ('5380806308794220','5123', 5, 3500.00),/*excede el limite en el día*/
		  ('5380806308794220','5123', 5, 3600.00),/*excede el limite en el día*/
		  ('5185491637214914','9156', 2,120.63),/*dos distintos postales en -5'*/
		  ('5185491637214914','9156', 19,220.75),/*dos distintos postales en -5'*/
		  ('4532938327362968','3630', 11,385.32),/*dos compras mismo postal -1'*/
		  ('4532938327362968','3630', 12,439.20);/*dos compras mismo postal -1'*/
		  
		`)
	if err != nil {
		log.Fatal(err)
		}
}

func insertarCompra() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=bd2 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	_, err = db.Exec(`
	insert into compra( nrooperacion, nrotarjeta, nrocomercio, fecha, monto,pagado ) 
	values (default,'5367984198314628',2,'2023-06-21 19:02:20',120.00,false),
		   (default,'5364052507608396',11,'2023-06-23 12:24:57',200.00,false),/*expirada*/
		   (default,'4716515488257399',3,'2023-06-23 16:30:34',69.30,false),
		   
		   (default,'5380806308794220',16,'2023-06-15 04:22:22',3500.00,false),/*excede el limite en el día*/
		   (default,'5380806308794220',3,'2023-06-15 17:48:36',3500.00,false),
		   
		   (default,'5185491637214914',2,'2023-05-19 15:30:20',520.00,false),/*dos compras distintos postales en -5'*/
		   (default,'5185491637214914',19,'2023-05-19 15:32:17',137.00,false),
		   
		   (default,'4532938327362968',11,'2023-06-22 11:02:20',120.00,false),/*dos compras mismo postal -1'*/
		   (default,'4532938327362968',12,'2023-06-22 11:02:36',600.00,false),
		   
		   (default,5367984198314628,2,'2023-06-21 11:59:11',620.00,false);
	

	`)
	if err != nil {
		log.Fatal(err)
	}
}





func authCompraSP(){
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=bd2 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
	create or replace function authCompra(numtarj char(16), cod char(4), nrocomercio int, _monto decimal(7,2)) returns boolean as $$
	declare
		tarj tarjeta%rowtype;
		suma decimal(7,2);
	begin
		select * into tarj from tarjeta where nrotarjeta = numtarj;
	
		if not found or tarj.estado = 'anulada' then
			insert into rechazo(nrotarjeta, nrocomercio, fecha, monto, motivo)
			values (numtarj, nrocomercio, NOW(), _monto, 'tarjeta no válida o no vigente.');
			return false;
		end if;
		
		if tarj.codseguridad != cod then
			insert into rechazo(nrotarjeta, nrocomercio, fecha, monto, motivo)
			values (numtarj, nrocomercio, NOW(), _monto, 'código de seguridad inválido.');
			return false;
		end if;
		
		if TO_DATE(tarj.validahasta, 'MMYYYY') <= CURRENT_DATE or tarj.estado = 'expirada' then
			insert into rechazo(nrotarjeta, nrocomercio, fecha, monto, motivo)
			values (numtarj, nrocomercio, NOW(), _monto, 'plazo de vigencia expirado.');
			update tarjeta set estado = 'expirada' where nrotarjeta = numtarj; 
			return false;
		end if;
		
		if tarj.estado = 'suspendida' then
			insert into rechazo(nrotarjeta, nrocomercio, fecha, monto, motivo)
			values (numtarj, nrocomercio, NOW(), _monto, 'la tarjeta se encuentra suspendida.');
			return false;
		end if;
		
		select sum(monto) into suma from compra where nrotarjeta = numtarj and pagado = false group by nrotarjeta;
		
		if _monto>=tarj.limitecompra or suma >= tarj.limitecompra then
			insert into rechazo(nrotarjeta, nrocomercio, fecha, monto, motivo)
			values (numtarj, nrocomercio, NOW(), _monto, 'límite de compra excedido.');
			return false;
		end if;
		
		insert into compra(nrotarjeta, nrocomercio, fecha, monto, pagado)
		values (numtarj, nrocomercio, NOW(), _monto, false);
		
		return true;
	end;
	$$language plpgsql;`)
	if err != nil {
		log.Fatal(err)
		}

 }

func emitirResumen() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=bd2 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	_, err = db.Exec(`
	create or replace function guardarResumen(nro_cliente int, mes_ int, anio_ int) returns void as $$
declare
    resumen_id int;
    nombre_cliente text;
    apellido_cliente text;
    domicilio_cliente text;
    nrotarjeta_cliente char(60);
    fecha_inicio date;
    fecha_fin date;
    fecha_vencimiento date;
    total_pagar decimal(8,2);
    tarjetaRow tarjeta%rowtype;
begin
    select nombre, apellido, domicilio
    into nombre_cliente, apellido_cliente, domicilio_cliente
    from cliente
    where nrocliente = nro_cliente;

    for tarjetaRow in select * from tarjeta where nrocliente = nro_cliente
    loop
        nrotarjeta_cliente := tarjetaRow.nrotarjeta;

        select fechainicio, fechacierre, fechavto
        into fecha_inicio, fecha_fin, fecha_vencimiento
        from cierre
        where mes = mes_ and año = anio_ and terminacion = cast(substring(nrotarjeta_cliente, length(nrotarjeta_cliente)) as int8);

        select coalesce(sum(monto), 0)
        into total_pagar
        from compra
        where nrotarjeta = nrotarjeta_cliente and date(fecha) >= fecha_inicio and date(fecha) <= fecha_fin;

        insert into cabecera(nroresumen, nombre, apellido, domicilio, nrotarjeta, desde, hasta, vence, total)
        values (default, nombre_cliente, apellido_cliente, domicilio_cliente, nrotarjeta_cliente, fecha_inicio, fecha_fin, fecha_vencimiento, total_pagar)
        returning nroresumen into resumen_id;

        insert into detalle(nroresumen, nrolinea, fecha, nombrecomercio, monto)
        select resumen_id, row_number() over(partition by resumen_id order by fecha), c.fecha, (select nombre from comercio where nrocomercio = c.nrocomercio), c.monto
        from compra c
        where nrotarjeta = nrotarjeta_cliente and fecha between fecha_inicio and fecha_fin;
    end loop;
end;
$$ language plpgsql;

	`)
	if err != nil {
		log.Fatal(err)
	
	}	
}

func alertasAClientes2() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=bd2 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	_, err = db.Exec(`
	create or replace function alertasAClientes2()
	returns trigger as $$
	declare
		_nrotarjeta char(16);
		cpActual char(8);
		cpAnterior char(8);
		fechaActual timestamp;
		fechaAnterior timestamp;
		cantidadDeCompras int;
	--	_nroRechazo int;
	begin
		_nrotarjeta := new.nrotarjeta;
		fechaActual := new.fecha;
		cpActual := (select codigopostal from comercio where nrocomercio = new.nrocomercio);
	
		select count(*) into cantidadDeCompras
		from compra c
		join comercio co on c.nrocomercio = co.nrocomercio
		where c.nrotarjeta = _nrotarjeta
			and co.codigopostal = cpActual
			and c.fecha < fechaActual
			and c.fecha >= fechaActual - interval '1 minute';
	
		if cantidadDeCompras > 0 then		
			insert into alerta (nrotarjeta,fecha,nrorechazo,codalerta,descripcion)
			values (new.nrotarjeta,NOW(),0,1,'Se han detectado dos compras en el mismo codigo postal dentro de un lapso de un minuto.');	
			return null;
		end if;
		
		select count(*) into cantidadDeCompras
		from compra c
		join comercio co on c.nrocomercio = co.nrocomercio
		where c.nrotarjeta = _nrotarjeta
			and co.codigopostal != cpActual
			and c.fecha < fechaActual
			and c.fecha >= fechaActual - interval '5 minute';
	
		if cantidadDeCompras > 0 then		
			insert into alerta (nrotarjeta,fecha,nrorechazo,codalerta,descripcion)
			values (new.nrotarjeta,NOW(),0,5,'Se han detectado dos compras en un codigo postal distinto dentro de un lapso de 5 minutos.');	
			return null;
		end if;
	
		return new;
	end;
	$$ language plpgsql;
	
	create trigger alertasAClientes2_trg
	before insert on compra 
	for each row execute procedure alertasAClientes2();
	`)

	if err != nil {
		log.Fatal(err)
	
	}	
} 
func suspenderTarjetaExcesoLimite(){
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=bd2 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	_, err = db.Exec(`
	create or replace function suspenderTarjetaExcesoLimite()
	returns trigger as $$
	declare
		_nrotarjeta char(16);
		_fecha date;
		cantidadRechazos int;
	begin
		_nrotarjeta := new.nrotarjeta;
		_fecha := cast(new.fecha as date);
	
		select count(*) into cantidadRechazos
		from rechazo r
		where r.nrotarjeta=_nrotarjeta
		and r.motivo = 'límite de compra excedido.'
		and cast(r.fecha as date) = _fecha;
	
		if cantidadRechazos >= 2 then
			update tarjeta t
			set estado = 'suspendida'
			where t.nrotarjeta=_nrotarjeta;
	
			insert into alerta (nrotarjeta,fecha,nrorechazo,codalerta,descripcion)
			values (_nrotarjeta,new.fecha,new.nrorechazo,32,'Tarjeta suspendida preventivamente');
		end if;
		return new;
	end;
	$$ language plpgsql;
	
	create trigger suspenderTarjetaExcesoLimite_trg
	after insert on rechazo 
	for each row execute procedure suspenderTarjetaExcesoLimite();
	`)
	if err != nil {
		log.Fatal(err)
	}
}

func alertaRechazo(){
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=bd2 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
	create or replace function alertaRechazo() returns trigger as $$
	begin
		insert into alerta (nrotarjeta,fecha,nrorechazo,codalerta,descripcion)
		values (new.nrotarjeta,NOW(),new.nrorechazo,0,new.motivo);	
		return new;
	end;
	$$language plpgsql;
	
	create trigger alertaRechazo_trg
	after insert on rechazo
	for each row execute procedure alertaRechazo();`)
	if err != nil {
		log.Fatal(err)
		}

 }
 

func probarConsumos(){
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=bd2 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	_, err = db.Exec(`
		DO $$
		DECLARE
			cons consumo%rowtype;
		BEGIN
			FOR cons IN SELECT nrotarjeta, codseguridad, nrocomercio, monto FROM consumo LOOP
				PERFORM authCompra(cons.nrotarjeta, cons.codseguridad, cons.nrocomercio, cons.monto);
			END LOOP;
		END $$;
	`)
	if err != nil {
		log.Fatal(err)
	}		
}

func borrarPKFK(){
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=bd2 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
	create or replace function BorrarPKFK() returns void as $$	
	begin
		alter table tarjeta drop constraint tarjeta_nrocliente_fk;
		alter table compra drop constraint compra_nrotarjeta_fk;
		alter table compra drop constraint compra_nrocomercio_fk;
		alter table rechazo drop constraint rechazo_nrocomercio_fk;
		alter table cabecera drop constraint cabecera_nrotarjeta_fk;
		alter table detalle drop constraint detalle_nroresumen_fk;
		alter table alerta drop constraint alerta_nrotarjeta_fk;
	
		alter table cliente drop constraint nrocliente_pk;
		alter table tarjeta drop constraint nrotarjeta_pk;
		alter table comercio drop constraint nrocomercio_pk;
		alter table compra drop constraint nrooperacion_pk;
		alter table rechazo drop constraint nrorechazo_pk;
		alter table cierre drop constraint cierre_pk;
		alter table cabecera drop constraint nroresumen_pk;
		alter table detalle drop constraint detalle_pk;
		alter table alerta drop constraint nroalerta_pk;
	end;
	$$language plpgsql;
	
	`)
	if err != nil {
		log.Fatal(err)
		}

 }
func exportarABoltDB() {
	dbPost, err := sql.Open("postgres", "user=postgres host=localhost dbname=bd2 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer dbPost.Close()

	db, err := bolt.Open("bolt.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	clientes, err := dbPost.Query(`SELECT * FROM cliente LIMIT 3`)
	if err != nil {
		log.Fatal(err)
	}
	defer clientes.Close()

	for clientes.Next() {
		var cliente Cliente
		err := clientes.Scan(&cliente.Nrocliente, &cliente.Nombre, &cliente.Apellido, &cliente.Domicilio, &cliente.Telefono)
		if err != nil {
			log.Fatal(err)
		}
		//fmt.Printf("cliente antes: %v\n",cliente)
		data, err := json.Marshal(cliente)
		if err != nil {
			fmt.Printf("error: %s\n",err)
			log.Fatal(err)
		}
		//fmt.Printf("cliente marshall: %s\n",string(data))
		err = CreateUpdate(db, "cliente", []byte(strconv.Itoa(cliente.Nrocliente)), data)
		if err != nil {
			fmt.Printf("error: %s\n",err)
			log.Fatal(err)
		}
		//resultado,err:=ReadUnique(db,"cliente",[]byte(strconv.Itoa(cliente.Nrocliente)))
		//fmt.Printf("cliente bucket: %s\n",string(resultado))
	}

	tarjetas, err := dbPost.Query(`SELECT * FROM tarjeta LIMIT 3`)
	if err != nil {
		log.Fatal(err)
	}
	defer tarjetas.Close()

	for tarjetas.Next() {
		var tarjeta Tarjeta
		err := tarjetas.Scan(&tarjeta.Nrotarjeta, &tarjeta.Nrocliente, &tarjeta.Validadesde, &tarjeta.Validahasta, &tarjeta.Codseguridad, &tarjeta.Limitecompra, &tarjeta.Estado)
		if err != nil {
			log.Fatal(err)
		}

		data, err := json.Marshal(tarjeta)
		if err != nil {
			log.Fatal(err)
		}

		err = CreateUpdate(db, "tarjeta", []byte(tarjeta.Nrotarjeta), data)
		if err != nil {
			log.Fatal(err)
		}
	}

	comercios, err := dbPost.Query(`SELECT * FROM comercio LIMIT 3`)
	if err != nil {
		log.Fatal(err)
	}
	defer comercios.Close()

	for comercios.Next() {
		var comercio Comercio
		err := comercios.Scan(&comercio.Nrocomercio, &comercio.Nombre, &comercio.Domicilio, &comercio.Codigopostal, &comercio.Telefono)
		if err != nil {
			log.Fatal(err)
		}

		data, err := json.Marshal(comercio)
		if err != nil {
			log.Fatal(err)
		}

		err = CreateUpdate(db, "comercio", []byte(strconv.Itoa(comercio.Nrocomercio)), data)
		if err != nil {
			log.Fatal(err)
		}
	}

	compras, err := dbPost.Query(`SELECT * FROM compra LIMIT 3`)
	if err != nil {
		log.Fatal(err)
	}
	defer compras.Close()

	for compras.Next() {
		var compra Compra
		err := compras.Scan(&compra.Nrooperacion, &compra.Nrotarjeta, &compra.Nrocomercio, &compra.Fecha, &compra.Monto, &compra.Pagado)
		if err != nil {
			log.Fatal(err)
		}

		data, err := json.Marshal(compra)
		if err != nil {
			log.Fatal(err)
		}

		err = CreateUpdate(db, "compra", []byte(strconv.FormatInt(compra.Nrooperacion, 10)), data)
		if err != nil {
			log.Fatal(err)
		}
	}

	rechazos, err := dbPost.Query(`SELECT * FROM rechazo LIMIT 3`)
	if err != nil {
		log.Fatal(err)
	}
	defer rechazos.Close()

	for rechazos.Next() {
		var rechazo Rechazo
		err := rechazos.Scan(&rechazo.Nrorechazo, &rechazo.Nrotarjeta, &rechazo.Nrocomercio, &rechazo.Fecha, &rechazo.Monto, &rechazo.Motivo)
		if err != nil {
			log.Fatal(err)
		}

		data, err := json.Marshal(rechazo)
		if err != nil {
			log.Fatal(err)
		}

		err = CreateUpdate(db, "rechazo", []byte(strconv.FormatInt(rechazo.Nrorechazo, 10)), data)
		if err != nil {
			log.Fatal(err)
		}
	}

	cierres, err := dbPost.Query(`SELECT * FROM cierre LIMIT 3`)
	if err != nil {
		log.Fatal(err)
	}
	defer cierres.Close()

	for cierres.Next() {
		var cierre Cierre
		err := cierres.Scan(&cierre.Año, &cierre.Mes, &cierre.Terminacion, &cierre.Fechainicio, &cierre.Fechacierre, &cierre.Fechavto)
		if err != nil {
			log.Fatal(err)
		}

		data, err := json.Marshal(cierre)
		if err != nil {
			log.Fatal(err)
		}

		err = CreateUpdate(db, "cierre", []byte(fmt.Sprintf("%d-%d-%d", cierre.Año, cierre.Mes, cierre.Terminacion)), data)
		if err != nil {
			log.Fatal(err)
		}
	}

	cabeceras, err := dbPost.Query(`SELECT * FROM cabecera LIMIT 3`)
	if err != nil {
		log.Fatal(err)
	}
	defer cabeceras.Close()

	for cabeceras.Next() {
		var cabecera Cabecera
		err := cabeceras.Scan(&cabecera.NroResumen, &cabecera.Nombre, &cabecera.Apellido, &cabecera.Domicilio, &cabecera.NroTarjeta, &cabecera.Desde, &cabecera.Hasta, &cabecera.Vence, &cabecera.Total)
		if err != nil {
			log.Fatal(err)
		}

		data, err := json.Marshal(cabecera)
		if err != nil {
			log.Fatal(err)
		}

		err = CreateUpdate(db, "cabecera", []byte(strconv.FormatInt(cabecera.NroResumen, 10)), data)
		if err != nil {
			log.Fatal(err)
		}
	}

	detalles, err := dbPost.Query(`SELECT * FROM detalle LIMIT 3`)
	if err != nil {
		log.Fatal(err)
	}
	defer detalles.Close()

	for detalles.Next() {
		var detalle Detalle
		err := detalles.Scan(&detalle.NroResumen, &detalle.NroLinea, &detalle.Fecha, &detalle.NombreComercio, &detalle.Monto)
		if err != nil {
			log.Fatal(err)
		}

		data, err := json.Marshal(detalle)
		if err != nil {
			log.Fatal(err)
		}

		err = CreateUpdate(db, "detalle", []byte(fmt.Sprintf("%d-%d", detalle.NroResumen, detalle.NroLinea)), data)
		if err != nil {
			log.Fatal(err)
		}
	}

	alertas, err := dbPost.Query(`SELECT * FROM alerta LIMIT 3`)
	if err != nil {
		log.Fatal(err)
	}
	defer alertas.Close()

	for alertas.Next() {
		var alerta Alerta
		err := alertas.Scan(&alerta.NroAlerta, &alerta.NroTarjeta, &alerta.Fecha, &alerta.NroRechazo, &alerta.CodAlerta, &alerta.Descripcion)
		if err != nil {
			log.Fatal(err)
		}

		data, err := json.Marshal(alerta)
		if err != nil {
			log.Fatal(err)
		}

		err = CreateUpdate(db, "alerta", []byte(strconv.FormatInt(alerta.NroAlerta, 10)), data)
		if err != nil {
			log.Fatal(err)
		}
	}

	consumos, err := dbPost.Query(`SELECT * FROM consumo LIMIT 3`)
	if err != nil {
		log.Fatal(err)
	}
	defer consumos.Close()

	for consumos.Next() {
		var consumo Consumo
		err := consumos.Scan(&consumo.NroTarjeta, &consumo.CodSeguridad, &consumo.NroComercio, &consumo.Monto)
		if err != nil {
			log.Fatal(err)
		}

		data, err := json.Marshal(consumo)
		if err != nil {
			log.Fatal(err)
		}

		err = CreateUpdate(db, "consumo", []byte(consumo.NroTarjeta), data)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func CreateUpdate(db *bolt.DB, bucketName string, key []byte, val []byte) error { 
	tx, err := db.Begin(true)
	if err != nil { 
		return err 
	}
	defer tx.Rollback() 
	b, _ := tx.CreateBucketIfNotExists([]byte(bucketName)) 
	
	err = b.Put(key, val)
	if err != nil { 
		return err 
	} 

	if err := tx.Commit(); err != nil { 
		return err 
	} 
	return nil 
} 

func ReadUnique(db *bolt.DB, bucketName string, key []byte) ([]byte, error) {
	var buf []byte 
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		buf = b.Get(key)
		return nil
	}) 
	return buf, err 
} 


