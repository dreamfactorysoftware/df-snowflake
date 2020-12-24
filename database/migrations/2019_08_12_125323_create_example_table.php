<?php

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Migrations\Migration;
use Symfony\Component\Console\Output\ConsoleOutput;

class CreateExampleTable extends Migration
{
    /**
     * Run the migrations. To create example database table.
     *
     * @return void
     */
    public function up()
    {
        $driver = Schema::getConnection()->getDriverName();
        // Even though we take care of this scenario in the code,
        // SQL Server does not allow potential cascading loops,
        // so set the default no action and clear out created/modified by another user when deleting a user.
        $onDelete = (('sqlsrv' === $driver) ? 'no action' : 'set null');

        $output = new ConsoleOutput();
        $output->writeln("Migration driver used: $driver");

        // Database Table Extras
        if (!Schema::hasTable('db_example')) {
            Schema::create(
                'db_example',
                function (Blueprint $t) use ($onDelete) {
                    $t->increments('id');
                    $t->integer('service_id')->unsigned();
                    $t->foreign('service_id')->references('id')->on('service')->onDelete('cascade');
                    $t->string('label')->nullable();
                    $t->text('description')->nullable();
                    $t->timestamp('created_date')->nullable();
                }
            );
        }
    }

    /**
     * Reverse the migrations.
     *
     * @return void
     */
    public function down()
    {
        // Drop created tables in reverse order

        // Database Extras
        Schema::dropIfExists('db_example');
    }
}
